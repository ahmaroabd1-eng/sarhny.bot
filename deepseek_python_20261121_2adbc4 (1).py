import asyncio
import json
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from html import escape

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import BaseFilter, Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)
from aiohttp import web
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import (
    BigInteger, Boolean, DateTime, ForeignKey, Integer, String, Text,
    func, select, update, desc,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# ============================================================
# CONFIG
# ============================================================


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    # Render هيقرأ المتغيرات دي من الـ Environment
    bot_token: str
    owner_id: int
    database_url: str = "sqlite+aiosqlite:///sarehni.db"


settings = Settings()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("sarehni")

# ============================================================
# DATABASE (هنستخدم SQLite مؤقتاً عشان البداية)
# ============================================================

engine = create_async_engine(settings.database_url, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    is_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    link: Mapped["PersonalLink"] = relationship(back_populates="user", uselist=False)


class PersonalLink(Base):
    __tablename__ = "personal_links"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), unique=True)
    slug: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    visits: Mapped[int] = mapped_column(Integer, default=0)
    messages_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped[User] = relationship(back_populates="link")


class MessageModel(Base):
    __tablename__ = "messages"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    receiver_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    sender_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    content_type: Mapped[str] = mapped_column(String(32), default="text")
    content: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)


class Report(Base):
    __tablename__ = "reports"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    message_id: Mapped[int] = mapped_column(Integer, ForeignKey("messages.id", ondelete="CASCADE"), index=True)
    reporter_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"))
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BotSetting(Base):
    __tablename__ = "bot_settings"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


# ============================================================
# SERVICES
# ============================================================


async def upsert_user(session: AsyncSession, tg_user) -> User:
    user = await session.get(User, tg_user.id)
    if user is None:
        user = User(
            id=tg_user.id, username=tg_user.username,
            first_name=tg_user.first_name, last_name=tg_user.last_name,
        )
        session.add(user)
    else:
        user.username = tg_user.username
        user.first_name = tg_user.first_name
        user.last_name = tg_user.last_name
        user.last_seen = datetime.now(timezone.utc)
    await session.commit()
    return user


async def get_user(session: AsyncSession, uid: int) -> User | None:
    return await session.get(User, uid)


async def get_or_create_link(session: AsyncSession, user_id: int) -> PersonalLink:
    link = (await session.execute(
        select(PersonalLink).where(PersonalLink.user_id == user_id)
    )).scalar_one_or_none()
    if link:
        return link
    link = PersonalLink(user_id=user_id, slug=secrets.token_urlsafe(8))
    session.add(link)
    await session.commit()
    await session.refresh(link)
    return link


async def get_link_by_slug(session: AsyncSession, slug: str) -> PersonalLink | None:
    return (await session.execute(
        select(PersonalLink).where(PersonalLink.slug == slug)
    )).scalar_one_or_none()


async def increment_visits(session: AsyncSession, link_id: int) -> None:
    await session.execute(
        update(PersonalLink).where(PersonalLink.id == link_id).values(visits=PersonalLink.visits + 1)
    )
    await session.commit()


async def toggle_link(session: AsyncSession, user_id: int) -> bool | None:
    link = (await session.execute(
        select(PersonalLink).where(PersonalLink.user_id == user_id)
    )).scalar_one_or_none()
    if not link:
        return None
    link.is_active = not link.is_active
    await session.commit()
    return link.is_active


async def create_message(
    session: AsyncSession, receiver_id: int, sender_id: int, content_type: str, content: str
) -> MessageModel:
    msg = MessageModel(
        receiver_id=receiver_id, sender_id=sender_id,
        content_type=content_type, content=content,
    )
    session.add(msg)
    await session.commit()
    await session.refresh(msg)
    link = (await session.execute(
        select(PersonalLink).where(PersonalLink.user_id == receiver_id)
    )).scalar_one_or_none()
    if link:
        link.messages_count += 1
        await session.commit()
    return msg


async def get_message(session: AsyncSession, mid: int) -> MessageModel | None:
    return await session.get(MessageModel, mid)


async def list_messages(session: AsyncSession, receiver_id: int, limit: int = 10):
    return (await session.execute(
        select(MessageModel, User)
        .outerjoin(User, User.id == MessageModel.sender_id)
        .where(MessageModel.receiver_id == receiver_id, MessageModel.is_deleted.is_(False))
        .order_by(desc(MessageModel.created_at))
        .limit(limit)
    )).all()


async def mark_deleted(session: AsyncSession, mid: int, receiver_id: int) -> bool:
    msg = await session.get(MessageModel, mid)
    if not msg or msg.receiver_id != receiver_id:
        return False
    msg.is_deleted = True
    await session.commit()
    return True


async def create_report(session: AsyncSession, mid: int, reporter_id: int, reason: str) -> Report:
    r = Report(message_id=mid, reporter_id=reporter_id, reason=reason)
    session.add(r)
    await session.commit()
    return r


async def list_pending_reports(session: AsyncSession):
    return (await session.execute(
        select(Report).where(Report.status == "pending").order_by(desc(Report.created_at))
    )).scalars().all()


async def get_setting(session: AsyncSession, key: str, default: str | None = None) -> str | None:
    s = await session.get(BotSetting, key)
    return s.value if s else default


async def set_setting(session: AsyncSession, key: str, value: str) -> None:
    s = await session.get(BotSetting, key)
    if s:
        s.value = value
    else:
        session.add(BotSetting(key=key, value=value))
    await session.commit()


async def get_dynamic_text(session: AsyncSession, key: str, default: str) -> str:
    val = await get_setting(session, key)
    return val if val else default


async def get_extra_buttons(session: AsyncSession) -> list:
    val = await get_setting(session, "extra_buttons")
    if val:
        try:
            return json.loads(val)
        except Exception:
            return []
    return []


async def save_extra_buttons(session: AsyncSession, buttons: list):
    await set_setting(session, "extra_buttons", json.dumps(buttons))


async def global_stats(session: AsyncSession) -> dict:
    return {
        "users": (await session.execute(select(func.count(User.id)))).scalar_one(),
        "blocked": (await session.execute(
            select(func.count(User.id)).where(User.is_blocked.is_(True))
        )).scalar_one(),
        "messages": (await session.execute(select(func.count(MessageModel.id)))).scalar_one(),
        "links": (await session.execute(select(func.count(PersonalLink.id)))).scalar_one(),
    }


async def user_stats(session: AsyncSession, uid: int) -> dict:
    received = (await session.execute(
        select(func.count(MessageModel.id)).where(
            MessageModel.receiver_id == uid, MessageModel.is_deleted.is_(False)
        )
    )).scalar_one()
    sent = (await session.execute(
        select(func.count(MessageModel.id)).where(MessageModel.sender_id == uid)
    )).scalar_one()
    link = (await session.execute(
        select(PersonalLink).where(PersonalLink.user_id == uid)
    )).scalar_one_or_none()
    return {"received": received, "sent": sent, "visits": link.visits if link else 0}


async def broadcast(bot: Bot, session: AsyncSession, text: str) -> tuple[int, int]:
    users = (await session.execute(
        select(User.id).where(User.is_active.is_(True), User.is_blocked.is_(False))
    )).scalars().all()
    sent = failed = 0
    for uid in users:
        try:
            await bot.send_message(uid, text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    return sent, failed


async def block_user(session: AsyncSession, uid: int, blocked: bool) -> bool:
    u = await session.get(User, uid)
    if not u:
        return False
    u.is_blocked = blocked
    await session.commit()
    return True


# ============================================================
# MIDDLEWARES
# ============================================================


class DbSessionMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        async with SessionLocal() as session:
            data["session"] = session
            return await handler(event, data)


class UserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        tg_user = None
        if hasattr(event, "message") and event.message:
            tg_user = event.message.from_user
        elif hasattr(event, "callback_query") and event.callback_query:
            tg_user = event.callback_query.from_user

        session: AsyncSession | None = data.get("session")
        if tg_user is not None and not tg_user.is_bot and session is not None:
            try:
                await upsert_user(session, tg_user)
            except Exception as e:
                logger.warning(f"upsert failed: {e}")
        return await handler(event, data)


class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, rate: int = 10, per: int = 60):
        self.rate = rate
        self.per = per
        self._hits: dict[int, list[float]] = {}

    async def __call__(self, handler, event, data):
        if not isinstance(event, Message) or event.from_user is None:
            return await handler(event, data)
        uid = event.from_user.id
        now = time.time()
        arr = self._hits.setdefault(uid, [])
        arr[:] = [t for t in arr if now - t < self.per]
        if len(arr) >= self.rate:
            await event.answer("⏳ بلاش سرعة، استنى شوية وجرب تاني.")
            return
        arr.append(now)
        return await handler(event, data)


class IsOwner(BaseFilter):
    async def __call__(self, event: Message | CallbackQuery) -> bool:
        return event.from_user is not None and event.from_user.id == settings.owner_id


# ============================================================
# STATES
# ============================================================


class SendAnonymous(StatesGroup):
    waiting = State()


class ReplyToMessage(StatesGroup):
    waiting = State()


class OwnerBroadcast(StatesGroup):
    waiting = State()


class OwnerSearch(StatesGroup):
    waiting = State()


class OwnerEditText(StatesGroup):
    waiting = State()


class OwnerAddButton(StatesGroup):
    waiting_text = State()
    waiting_url = State()


# ============================================================
# TEXTS / HELPERS
# ============================================================


WELCOME = (
    "مرحبًا بك في <b>صارحني</b> ❤️\n\n"
    "هنا يمكنك استقبال الرسائل الصريحة والمجهولة من الآخرين دون معرفة هويتهم.\n\n"
    "اختر من القائمة:"
)
ANON_INTRO = (
    "أنت الآن على وشك إرسال رسالة مجهولة ❤️\n\n"
    "اكتب رسالتك بكل صراحة، وستصل إلى صاحب الرابط دون إظهار هويتك."
)
ANON_SENT = "تم إرسال رسالتك بنجاح ❤️\nهويتك غير ظاهرة لصاحب الرابط."


def sender_line(sender: User | None) -> str:
    if not sender:
        return ""
    name = " ".join(p for p in [sender.first_name or "", sender.last_name or ""] if p) or "مستخدم"
    uname = f"@{sender.username}" if sender.username else "—"
    return (
        "\n\n—————\n"
        f"👤 الاسم: {escape(name)}\n"
        f"🔗 اليوزر: {escape(uname)}\n"
        f"🆔 الإيدي: <code>{sender.id}</code>"
    )


async def deliver_message(bot: Bot, chat_id: int, msg: MessageModel, sender: User | None, kb):
    header = f"📩 <b>رسالة #{msg.id}</b>"
    tail = sender_line(sender)
    try:
        if msg.content_type == "text":
            await bot.send_message(chat_id, f"{header}\n\n{escape(msg.content)}{tail}", reply_markup=kb)
        elif msg.content_type == "photo":
            await bot.send_photo(chat_id, msg.content, caption=header + tail, reply_markup=kb)
        elif msg.content_type == "sticker":
            await bot.send_sticker(chat_id, msg.content)
            await bot.send_message(chat_id, header + tail, reply_markup=kb)
        elif msg.content_type == "video":
            await bot.send_video(chat_id, msg.content, caption=header + tail, reply_markup=kb)
        elif msg.content_type == "voice":
            await bot.send_voice(chat_id, msg.content, caption=header + tail, reply_markup=kb)
        elif msg.content_type == "document":
            await bot.send_document(chat_id, msg.content, caption=header + tail, reply_markup=kb)
        else:
            await bot.send_message(chat_id, f"{header}\n\n{escape(msg.content)}{tail}", reply_markup=kb)
    except Exception as e:
        logger.warning(f"deliver failed: {e}")


# ============================================================
# KEYBOARDS
# ============================================================


def main_menu(owner: bool = False, extra_buttons: list = None) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🔗 رابط صارحني", callback_data="menu:link")],
        [InlineKeyboardButton(text="📩 رسائلي الواردة", callback_data="menu:inbox")],
        [InlineKeyboardButton(text="📊 إحصائياتي", callback_data="menu:stats")],
        [InlineKeyboardButton(text="⚙️ إعداداتي", callback_data="menu:settings")],
        [InlineKeyboardButton(text="ℹ️ حول البوت", callback_data="menu:about")],
    ]
    if extra_buttons:
        for btn in extra_buttons:
            if "text" in btn and "url" in btn:
                rows.append([InlineKeyboardButton(text=btn["text"], url=btn["url"])])
                
    if owner:
        rows.append([InlineKeyboardButton(text="🛠 لوحة التحكم", callback_data="menu:owner")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_kb(target: str = "menu:main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩️ رجوع", callback_data=target)]
    ])


def link_kb(active: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="⏸ تعطيل الاستقبال" if active else "▶️ تفعيل الاستقبال",
            callback_data="link:toggle",
        )],
        [InlineKeyboardButton(text="📋 إرسال الرابط لنسخه", callback_data="link:copy")],
        [InlineKeyboardButton(text="↩️ رجوع", callback_data="menu:main")],
    ])


def msg_actions(mid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="↩️ رد", callback_data=f"msg:reply:{mid}"),
            InlineKeyboardButton(text="🚩 إبلاغ", callback_data=f"msg:report:{mid}"),
        ],
        [InlineKeyboardButton(text="🗑 حذف", callback_data=f"msg:delete:{mid}")],
    ])


def report_kb(mid: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="إساءة / تحرش", callback_data=f"report:harass:{mid}")],
        [InlineKeyboardButton(text="سبام", callback_data=f"report:spam:{mid}")],
        [InlineKeyboardButton(text="تهديد", callback_data=f"report:threat:{mid}")],
        [InlineKeyboardButton(text="أخرى", callback_data=f"report:other:{mid}")],
        [InlineKeyboardButton(text="↩️ رجوع", callback_data=f"msg:back:{mid}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def owner_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 إحصائيات عامة", callback_data="owner:stats")],
        [InlineKeyboardButton(text="👥 بحث عن مستخدم", callback_data="owner:search")],
        [InlineKeyboardButton(text="📢 رسالة جماعية", callback_data="owner:broadcast")],
        [InlineKeyboardButton(text="🚩 البلاغات", callback_data="owner:reports")],
        [InlineKeyboardButton(text="✏️ تعديل النصوص", callback_data="owner:edit_texts")],
        [InlineKeyboardButton(text="➕ إدارة الأزرار", callback_data="owner:manage_buttons")],
        [InlineKeyboardButton(text="↩️ رجوع", callback_data="menu:main")],
    ])


def confirm_kb(action: str, uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ تأكيد", callback_data=f"confirm:{action}:{uid}")],
        [InlineKeyboardButton(text="❌ إلغاء", callback_data="owner:panel")],
    ])


# ============================================================
# HANDLERS — ANONYMOUS
# ============================================================

anon_router = Router(name="anon")


@anon_router.message(CommandStart(deep_link=True))
async def open_link(message: Message, command: CommandObject, state: FSMContext, session: AsyncSession):
    slug = command.args
    if not slug:
        return
    link = await get_link_by_slug(session, slug)
    if not link or not link.is_active:
        await message.answer("⚠️ هذا الرابط غير مفعّل حالياً.")
        return
    if link.user_id == message.from_user.id:
        await message.answer("ده رابطك إنت — شاركه مع أصدقائك عشان يبعتولك رسايل مجهولة.")
        return
    await increment_visits(session, link.id)
    await state.set_state(SendAnonymous.waiting)
    await state.update_data(receiver_id=link.user_id)
    
    intro_text = await get_dynamic_text(session, "text:anon_intro", ANON_INTRO)
    try:
        await message.answer(intro_text)
    except Exception:
        await message.answer(ANON_INTRO)


@anon_router.message(SendAnonymous.waiting, F.text | F.photo | F.sticker | F.video | F.voice | F.document)
async def receive_anon(message: Message, state: FSMContext, session: AsyncSession):
    data = await state.get_data()
    receiver_id = data.get("receiver_id")
    if not receiver_id:
        await state.clear()
        return

    ctype, content = "text", message.text or message.caption or ""
    if message.photo:
        ctype, content = "photo", message.photo[-1].file_id
    elif message.sticker:
        ctype, content = "sticker", message.sticker.file_id
    elif message.video:
        ctype, content = "video", message.video.file_id
    elif message.voice:
        ctype, content = "voice", message.voice.file_id
    elif message.document:
        ctype, content = "document", message.document.file_id

    if not content:
        await message.answer("ابعت رسالة نصية أو وسائط.")
        return

    msg = await create_message(session, receiver_id, message.from_user.id, ctype, content)
    sender = await get_user(session, message.from_user.id)
    await deliver_message(message.bot, receiver_id, msg, sender, msg_actions(msg.id))
    await message.answer(ANON_SENT)
    await state.clear()


@anon_router.message(SendAnonymous.waiting)
async def wrong_input(message: Message):
    await message.answer("ابعت رسالة نصية أو وسائط.")


# ============================================================
# HANDLERS — USER
# ============================================================

user_router = Router(name="user")


@user_router.message(CommandStart())
async def start(message: Message, state: FSMContext, session: AsyncSession):
    await state.clear()
    welcome_text = await get_dynamic_text(session, "text:welcome", WELCOME)
    extras = await get_extra_buttons(session)
    kb = main_menu(owner=(message.from_user.id == settings.owner_id), extra_buttons=extras)
    
    try:
        await message.answer(welcome_text, reply_markup=kb)
    except Exception:
        await message.answer(WELCOME, reply_markup=kb)


@user_router.callback_query(F.data == "menu:main")
async def menu_main(cb: CallbackQuery, state: FSMContext, session: AsyncSession):
    await state.clear()
    welcome_text = await get_dynamic_text(session, "text:welcome", WELCOME)
    extras = await get_extra_buttons(session)
    kb = main_menu(owner=(cb.from_user.id == settings.owner_id), extra_buttons=extras)
    
    try:
        await cb.message.edit_text(welcome_text, reply_markup=kb)
    except Exception:
        await cb.message.edit_text(WELCOME, reply_markup=kb)
    await cb.answer()


@user_router.callback_query(F.data == "menu:link")
async def menu_link(cb: CallbackQuery, session: AsyncSession):
    link = await get_or_create_link(session, cb.from_user.id)
    bot_info = await cb.bot.get_me()
    url = f"https://t.me/{bot_info.username}?start={link.slug}"
    text = (
        f"🔗 <b>رابطك الشخصي:</b>\n<code>{url}</code>\n\n"
        "شارك الرابط مع أصدقائك، وسيتمكنون من إرسال رسائل مجهولة لك.\n\n"
        f"حالة الاستقبال: {'✅ مفعّل' if link.is_active else '⏸ معطّل'}\n"
        f"عدد الزيارات: {link.visits}\n"
        f"عدد الرسائل: {link.messages_count}"
    )
    await cb.message.edit_text(text, reply_markup=link_kb(link.is_active), disable_web_page_preview=True)
    await cb.answer()


@user_router.callback_query(F.data == "link:toggle")
async def link_toggle(cb: CallbackQuery, session: AsyncSession):
    active = await toggle_link(session, cb.from_user.id)
    if active is None:
        await cb.answer("الرابط غير موجود", show_alert=True)
        return
    await menu_link(cb, session)


@user_router.callback_query(F.data == "link:copy")
async def link_copy(cb: CallbackQuery, session: AsyncSession):
    link = await get_or_create_link(session, cb.from_user.id)
    bot_info = await cb.bot.get_me()
    url = f"https://t.me/{bot_info.username}?start={link.slug}"
    await cb.message.answer(f"<code>{url}</code>")
    await cb.answer("اضغط على الرابط مطولاً لنسخه")


@user_router.callback_query(F.data == "menu:inbox")
async def menu_inbox(cb: CallbackQuery, session: AsyncSession):
    rows = await list_messages(session, cb.from_user.id, limit=10)
    if not rows:
        await cb.message.edit_text("📭 لا توجد رسائل حتى الآن.", reply_markup=back_kb())
        await cb.answer()
        return
    await cb.message.edit_text("📩 آخر رسائلك:", reply_markup=back_kb())
    for msg, sender in rows:
        await deliver_message(cb.bot, cb.from_user.id, msg, sender, msg_actions(msg.id))
    await cb.answer()


@user_router.callback_query(F.data.startswith("msg:delete:"))
async def msg_delete(cb: CallbackQuery, session: AsyncSession):
    mid = int(cb.data.split(":")[2])
    if await mark_deleted(session, mid, cb.from_user.id):
        await cb.message.edit_text("🗑 تم حذف الرسالة.")
    else:
        await cb.answer("تعذر الحذف", show_alert=True)
    await cb.answer()


@user_router.callback_query(F.data.startswith("msg:reply:"))
async def msg_reply_start(cb: CallbackQuery, state: FSMContext, session: AsyncSession):
    mid = int(cb.data.split(":")[2])
    msg = await get_message(session, mid)
    if not msg or msg.receiver_id != cb.from_user.id:
        await cb.answer("تعذر الوصول", show_alert=True)
        return
    await state.set_state(ReplyToMessage.waiting)
    await state.update_data(reply_mid=mid)
    await cb.message.answer("✍️ اكتب ردك دلوقتي، هيتبعت لصاحب الرسالة من غير ما يعرف هويتك.")
    await cb.answer()


@user_router.message(ReplyToMessage.waiting, F.text)
async def msg_reply_send(message: Message, state: FSMContext, session: AsyncSession):
    data = await state.get_data()
    mid = data.get("reply_mid")
    if not mid:
        await state.clear()
        return
    original = await get_message(session, mid)
    if not original or not original.sender_id:
        await message.answer("تعذر إرسال الرد.")
        await state.clear()
        return
    if await get_setting(session, f"replies_disabled:{original.sender_id}", "false") == "true":
        await message.answer("❌ هذا المستخدم معطّل استقبال الردود.")
        await state.clear()
        return
    try:
        await message.bot.send_message(
            original.sender_id,
            f"📨 <b>رد على رسالتك</b>\n\n{escape(message.text)}",
        )
        await message.answer("✅ تم إرسال الرد.")
    except Exception:
        await message.answer("❌ تعذر إرسال الرد — يمكن المستخدم عامل بلوك للبوت.")
    await state.clear()


@user_router.callback_query(F.data.startswith("msg:report:"))
async def msg_report_start(cb: CallbackQuery, session: AsyncSession):
    mid = int(cb.data.split(":")[2])
    msg = await get_message(session, mid)
    if not msg or msg.receiver_id != cb.from_user.id:
        await cb.answer("تعذر الوصول", show_alert=True)
        return
    await cb.message.edit_text("اختر سبب الإبلاغ:", reply_markup=report_kb(mid))
    await cb.answer()


@user_router.callback_query(F.data.startswith("report:"))
async def msg_report_submit(cb: CallbackQuery, session: AsyncSession):
    _, reason, mid_s = cb.data.split(":")
    mid = int(mid_s)
    msg = await get_message(session, mid)
    if not msg or msg.receiver_id != cb.from_user.id:
        await cb.answer("تعذر الوصول", show_alert=True)
        return
    await create_report(session, mid, cb.from_user.id, reason)
    await cb.message.edit_text("🚩 تم إرسال البلاغ. شكراً لك.")
    await cb.answer()


@user_router.callback_query(F.data.startswith("msg:back:"))
async def msg_back(cb: CallbackQuery, session: AsyncSession):
    mid = int(cb.data.split(":")[2])
    msg = await get_message(session, mid)
    if not msg or msg.receiver_id != cb.from_user.id:
        await cb.answer("غير موجود", show_alert=True)
        return
    sender = await get_user(session, msg.sender_id) if msg.sender_id else None
    await deliver_message(cb.bot, cb.from_user.id, msg, sender, msg_actions(msg.id))
    await cb.message.delete()
    await cb.answer()


@user_router.callback_query(F.data == "menu:stats")
async def menu_stats(cb: CallbackQuery, session: AsyncSession):
    s = await user_stats(session, cb.from_user.id)
    text = (
        "📊 <b>إحصائياتك</b>\n\n"
        f"📩 رسائل مستلمة: {s['received']}\n"
        f"📤 رسائل مرسلة: {s['sent']}\n"
        f"👁 زيارات رابطك: {s['visits']}\n"
    )
    await cb.message.edit_text(text, reply_markup=back_kb())
    await cb.answer()


@user_router.callback_query(F.data == "menu:settings")
async def menu_settings(cb: CallbackQuery, session: AsyncSession):
    disabled = await get_setting(session, f"replies_disabled:{cb.from_user.id}", "false")
    text = (
        "⚙️ <b>إعداداتك</b>\n\n"
        "استقبال الردود على رسائلك المجهولة: "
        + ("❌ معطّل" if disabled == "true" else "✅ مفعّل")
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🔕 تعطيل/تفعيل استقبال الردود",
            callback_data="settings:toggle_replies",
        )],
        [InlineKeyboardButton(text="↩️ رجوع", callback_data="menu:main")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@user_router.callback_query(F.data == "settings:toggle_replies")
async def settings_toggle_replies(cb: CallbackQuery, session: AsyncSession):
    key = f"replies_disabled:{cb.from_user.id}"
    cur = await get_setting(session, key, "false")
    await set_setting(session, key, "false" if cur == "true" else "true")
    await menu_settings(cb, session)


@user_router.callback_query(F.data == "menu:about")
async def menu_about(cb: CallbackQuery):
    await cb.message.edit_text(
        "ℹ️ <b>حول البوت</b>\n\n"
        "صارحني هو بوت رسائل مجهولة.\n"
        "أنشئ رابطك الشخصي وشاركه مع أصدقائك لاستقبال رسائل صريحة ومجهولة.\n\n"
        "🔒 <b>الخصوصية:</b> هوية المرسل مخفية عنك.",
        reply_markup=back_kb(),
    )
    await cb.answer()


# ============================================================
# HANDLERS — OWNER
# ============================================================

owner_router = Router(name="owner")
owner_router.message.filter(IsOwner())
owner_router.callback_query.filter(IsOwner())


@owner_router.message(Command("owner"))
async def owner_cmd(message: Message):
    await message.answer("🛠 لوحة التحكم", reply_markup=owner_kb())


@owner_router.callback_query(F.data.in_({"menu:owner", "owner:panel"}))
async def owner_panel(cb: CallbackQuery):
    await cb.message.edit_text("🛠 لوحة التحكم", reply_markup=owner_kb())
    await cb.answer()


@owner_router.callback_query(F.data == "owner:stats")
async def owner_stats(cb: CallbackQuery, session: AsyncSession):
    s = await global_stats(session)
    text = (
        "📊 <b>إحصائيات عامة</b>\n\n"
        f"👥 المستخدمين: {s['users']}\n"
        f"🚫 المحظورين: {s['blocked']}\n"
        f"📩 إجمالي الرسائل: {s['messages']}\n"
        f"🔗 الروابط: {s['links']}\n"
    )
    await cb.message.edit_text(text, reply_markup=back_kb("owner:panel"))
    await cb.answer()


@owner_router.callback_query(F.data == "owner:search")
async def owner_search_start(cb: CallbackQuery, state: FSMContext):
    await state.set_state(OwnerSearch.waiting)
    await cb.message.edit_text("🔍 اكتب Telegram ID للمستخدم:", reply_markup=back_kb("owner:panel"))
    await cb.answer()


@owner_router.message(OwnerSearch.waiting, F.text)
async def owner_search_run(message: Message, state: FSMContext, session: AsyncSession):
    try:
        uid = int(message.text.strip())
    except ValueError:
        await message.answer("❌ ID غير صالح.")
        return
    u = await get_user(session, uid)
    if not u:
        await message.answer("❌ المستخدم غير موجود.")
        await state.clear()
        return
    text = (
        f"👤 <b>{(u.first_name or '')} {(u.last_name or '')}</b>\n"
        f"🆔 <code>{u.id}</code>\n"
        f"🔗 @{u.username or '—'}\n"
        f"🚫 محظور: {'نعم' if u.is_blocked else 'لا'}\n"
        f"📅 انضم: {u.created_at:%Y-%m-%d}\n"
    )
    await message.answer(text, reply_markup=confirm_kb("toggle_block", u.id))
    await state.clear()


@owner_router.callback_query(F.data.startswith("confirm:toggle_block:"))
async def owner_toggle_block(cb: CallbackQuery, session: AsyncSession):
    uid = int(cb.data.split(":")[2])
    u = await get_user(session, uid)
    if not u:
        await cb.answer("غير موجود", show_alert=True)
        return
    was_blocked = u.is_blocked
    await block_user(session, uid, not was_blocked)
    await cb.message.edit_text(
        f"{'🚫 تم حظر' if not was_blocked else '✅ تم إلغاء حظر'} {uid}."
    )
    await cb.answer()


@owner_router.callback_query(F.data == "owner:broadcast")
async def owner_broadcast_start(cb: CallbackQuery, state: FSMContext):
    await state.set_state(OwnerBroadcast.waiting)
    await cb.message.edit_text(
        "📢 اكتب الرسالة اللي عايز تبعتها لكل المستخدمين:",
        reply_markup=back_kb("owner:panel"),
    )
    await cb.answer()


@owner_router.message(OwnerBroadcast.waiting, F.text)
async def owner_broadcast_run(message: Message, state: FSMContext, session: AsyncSession):
    await message.answer("⏳ جاري الإرسال...")
    sent, failed = await broadcast(message.bot, session, message.text or "")
    await message.answer(f"✅ تم الإرسال: {sent}\n❌ فشل: {failed}")
    await state.clear()


@owner_router.callback_query(F.data == "owner:reports")
async def owner_reports(cb: CallbackQuery, session: AsyncSession):
    reports = await list_pending_reports(session)
    if not reports:
        await cb.message.edit_text("لا توجد بلاغات.", reply_markup=back_kb("owner:panel"))
        await cb.answer()
        return
    await cb.message.edit_text("🚩 البلاغات المعلقة:", reply_markup=back_kb("owner:panel"))
    for r in reports:
        await cb.message.answer(
            f"بلاغ #{r.id}\nرسالة: {r.message_id}\nالسبب: {r.reason}\nالحالة: {r.status}"
        )
    await cb.answer()


# --- Edit Texts ---

@owner_router.callback_query(F.data == "owner:edit_texts")
async def owner_edit_texts(cb: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="رسالة الترحيب", callback_data="edittext:welcome")],
        [InlineKeyboardButton(text="رسالة المرسل المجهول", callback_data="edittext:anon_intro")],
        [InlineKeyboardButton(text="↩️ رجوع", callback_data="owner:panel")]
    ])
    await cb.message.edit_text("اختر النص اللي عايز تعدله:", reply_markup=kb)
    await cb.answer()


@owner_router.callback_query(F.data.startswith("edittext:"))
async def owner_edit_text_start(cb: CallbackQuery, state: FSMContext):
    key = cb.data.split(":")[1]
    await state.set_state(OwnerEditText.waiting)
    await state.update_data(edit_key=f"text:{key}")
    await cb.message.edit_text("✍️ اكتب النص الجديد دلوقتي وابعته:")
    await cb.answer()


@owner_router.message(OwnerEditText.waiting, F.text)
async def owner_edit_text_save(message: Message, state: FSMContext, session: AsyncSession):
    data = await state.get_data()
    key = data.get("edit_key")
    
    try:
        await message.bot.send_message(message.from_user.id, message.text)
    except Exception:
        await message.answer("❌ النص اللي كتبته فيه رموز مش مدعومة في تيليجرام (زي <escape> أو قوسين غلط).\nجرب تكتبه تاني بشكل صحيح:")
        return 
        
    await set_setting(session, key, message.text)
    await message.answer("✅ تم تعديل النص بنجاح.", reply_markup=owner_kb())
    await state.clear()


# --- Manage Buttons ---

@owner_router.callback_query(F.data == "owner:manage_buttons")
async def owner_manage_buttons(cb: CallbackQuery, session: AsyncSession):
    buttons = await get_extra_buttons(session)
    text = "➕ <b>إدارة الأزرار</b>\n\n"
    if buttons:
        text += "الأزرار الحالية:\n"
        for b in buttons:
            text += f"- {b['text']} ({b['url']})\n"
    else:
        text += "لا توجد أزرار إضافية حالياً.\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ إضافة زر جديد", callback_data="addbutton:start")],
        [InlineKeyboardButton(text="🗑 حذف كل الأزرار", callback_data="addbutton:clear")],
        [InlineKeyboardButton(text="↩️ رجوع", callback_data="owner:panel")]
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@owner_router.callback_query(F.data == "addbutton:start")
async def owner_add_button_start(cb: CallbackQuery, state: FSMContext):
    await state.set_state(OwnerAddButton.waiting_text)
    await cb.message.edit_text("1️⃣ اكتب اسم الزر (مثال: قناتي):")
    await cb.answer()


@owner_router.message(OwnerAddButton.waiting_text, F.text)
async def owner_add_button_text(message: Message, state: FSMContext):
    await state.update_data(btn_text=message.text)
    await state.set_state(OwnerAddButton.waiting_url)
    await message.answer("2️⃣ اكتب الرابط (مثال: https://t.me/yourchannel)\n\n⚠️ لازم يكون رابط صحيح.")


@owner_router.message(OwnerAddButton.waiting_url, F.text)
async def owner_add_button_url(message: Message, state: FSMContext, session: AsyncSession):
    url = message.text.strip()
    if not url.startswith("http"):
        await message.answer("❌ رابط غير صالح. لازم يبدأ بـ http أو https.")
        return
    data = await state.get_data()
    btn_text = data.get("btn_text")
    buttons = await get_extra_buttons(session)
    buttons.append({"text": btn_text, "url": url})
    await save_extra_buttons(session, buttons)
    await message.answer("✅ تم إضافة الزر بنجاح.", reply_markup=owner_kb())
    await state.clear()


@owner_router.callback_query(F.data == "addbutton:clear")
async def owner_add_button_clear(cb: CallbackQuery, session: AsyncSession):
    await save_extra_buttons(session, [])
    await cb.message.edit_text("🗑 تم حذف كل الأزرار.", reply_markup=owner_kb())
    await cb.answer()


# ============================================================
# BOOTSTRAP (Modified for Render Web Service)
# ============================================================


async def on_startup(bot: Bot):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Bot started in polling mode")


async def on_shutdown(bot: Bot):
    await engine.dispose()


async def health_check(request):
    return web.Response(text="Bot is running!")


async def main():
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.update.middleware(DbSessionMiddleware())
    dp.update.middleware(UserMiddleware())
    dp.message.middleware(ThrottlingMiddleware(rate=10))
    dp.include_router(owner_router)
    dp.include_router(anon_router)
    dp.include_router(user_router)

    await on_startup(bot)
    
    # تشغيل البوت في الخلفية
    asyncio.create_task(dp.start_polling(bot))
    
    # تشغيل سيرفر ويب وهمي عشان Render ميقفلش البوت
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}")

    # إبقاء السيرفر شغال
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        await runner.cleanup()
        await on_shutdown(bot)


if __name__ == "__main__":
    asyncio.run(main())