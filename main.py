# -*- coding: utf-8 -*-
# Souls Guard Bot — single file (FULL, text-only commands)
# Python 3.11+
import asyncio
import logging
import os
import re
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode, ChatType
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove, ChatMemberUpdated
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

APP_VERSION = "2025-08-31-text"
STARTED_AT = datetime.utcnow()

# -------- Optional Telethon (userbot) ----------
ENABLE_TELETHON = os.getenv("ENABLE_TELETHON", "0") == "1"
if ENABLE_TELETHON:
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession
    TELETHON_SESSION = os.getenv("TELETHON_SESSION", "")
    API_ID = int(os.getenv("API_ID", "0") or "0")
    API_HASH = os.getenv("API_HASH", "")

# --------------------------- ENV & GLOBALS ---------------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
OWNER_ID = int(os.environ["OWNER_ID"])
MAIN_CHAT_ID = int(os.environ["MAIN_CHAT_ID"])
GUARD_CHAT_ID = int(os.environ["GUARD_CHAT_ID"])
TEHRAN = ZoneInfo(os.getenv("TZ", "Asia/Tehran"))

ROLES = {
    "owner",
    "senior_chat", "senior_call", "senior_all",
    "admin_chat", "admin_call", "admin_channel",
    "member"
}

ROLE_ORDER = {
    "owner": 0,
    "senior_all": 1,
    "senior_chat": 2,
    "senior_call": 3,
    "admin_channel": 4,
    "admin_chat": 5,
    "admin_call": 6,
    "member": 99
}

# مجوز نقش‌ها
ALLOWED_VOICE_ROLES = {
    "owner", "senior_all", "senior_call", "admin_call",
    "senior_chat", "admin_chat",
}
ALLOWED_CHAT_ROLES = {
    "owner", "senior_all", "senior_chat", "admin_chat"
}

# وضعیت‌های موقت
PENDING_REPORT = {}
PENDING_CONTACT_OWNER = set()
PENDING_CONTACT_GUARD = set()
CALL_HEARTBEATS = {}  # {user_id: datetime}

# ------------------------------ Logging --------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("souls-bot")

# ----------------------------- Utilities -------------------------------------
def now_teh() -> datetime:
    return datetime.now(tz=TEHRAN)

def today_teh() -> date:
    return now_teh().date()

def is_admin_role(role: str) -> bool:
    return role in {
        "owner", "senior_chat", "senior_call", "senior_all",
        "admin_chat", "admin_call", "admin_channel"
    }

def pretty_td(seconds: int) -> str:
    if seconds < 0: seconds = 0
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def role_title(role: str) -> str:
    return {
        "owner": "👑 مالک",
        "senior_all": "🛡️ ارشد کل",
        "senior_chat": "🛡️ ارشد چت",
        "senior_call": "🛡️ ارشد کال",
        "admin_channel": "📢 ادمین کانال",
        "admin_chat": "💬 ادمین چت",
        "admin_call": "🎙️ ادمین کال",
        "member": "👤 عضو"
    }.get(role, role)

def src_tag(chat_id: int) -> str:
    return f"src:{chat_id}"

def uptime_str() -> str:
    d = datetime.utcnow() - STARTED_AT
    s = int(d.total_seconds())
    return pretty_td(s)

# ----------------------------- Database --------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users(
    user_id BIGINT PRIMARY KEY,
    username TEXT, first_name TEXT, last_name TEXT,
    role TEXT DEFAULT 'member',
    rank INT DEFAULT 0,
    joined_guard_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS groups(
    group_type TEXT PRIMARY KEY, -- 'main' / 'guard'
    chat_id BIGINT,
    title TEXT
);

CREATE TABLE IF NOT EXISTS sessions(
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
    kind TEXT CHECK (kind IN ('chat','call')) NOT NULL,
    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ,
    last_activity TIMESTAMPTZ,
    start_date DATE NOT NULL,
    source TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_open_session
ON sessions (user_id, kind) WHERE end_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_sessions_open ON sessions(user_id, kind) WHERE end_at IS NULL;

CREATE TABLE IF NOT EXISTS chat_metrics(
    user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
    d DATE NOT NULL,
    msgs INT DEFAULT 0,
    replies_sent INT DEFAULT 0,
    replies_received INT DEFAULT 0,
    PRIMARY KEY (user_id, d)
);

CREATE TABLE IF NOT EXISTS feedback(
    id BIGSERIAL PRIMARY KEY,
    target_user_id BIGINT,
    giver_user_id BIGINT,
    d DATE NOT NULL,
    score INT,
    context TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bans(
    user_id BIGINT PRIMARY KEY,
    reason TEXT,
    banned_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS candidates_daily(
    user_id BIGINT,
    d DATE NOT NULL,
    chat_msgs INT DEFAULT 0,
    call_seconds INT DEFAULT 0,
    presence_seconds INT DEFAULT 0,
    PRIMARY KEY (user_id, d)
);

CREATE TABLE IF NOT EXISTS first_prompt_main(
    user_id BIGINT,
    d DATE NOT NULL,
    PRIMARY KEY (user_id, d)
);

CREATE INDEX IF NOT EXISTS idx_chat_metrics_user_d ON chat_metrics(user_id, d);
CREATE INDEX IF NOT EXISTS idx_sessions_date_kind_user ON sessions(start_date, kind, user_id);
"""

async def ensure_user(pool, u):
    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO users(user_id, username, first_name, last_name)
            VALUES($1,$2,$3,$4)
            ON CONFLICT (user_id) DO UPDATE SET
                username=EXCLUDED.username,
                first_name=EXCLUDED.first_name,
                last_name=EXCLUDED.last_name,
                updated_at=now()
        """, u.id, (u.username or ""), (u.first_name or ""), (u.last_name or ""))

async def get_role(pool, user_id: int) -> str:
    async with pool.acquire() as con:
        r = await con.fetchval("SELECT role FROM users WHERE user_id=$1", user_id)
        return r or "member"

async def set_role(pool, user_id: int, role: str):
    if role not in ROLES: return False
    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO users(user_id, role, joined_guard_at)
            VALUES($1,$2,now())
            ON CONFLICT (user_id) DO UPDATE SET role=EXCLUDED.role,
            joined_guard_at = COALESCE(users.joined_guard_at, now())
        """, user_id, role)
    return True

async def open_session(pool, user_id: int, kind: str, source: str=None):
    d = today_teh()
    t = now_teh()
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE sessions SET end_at=now(), last_activity=now()
            WHERE user_id=$1 AND kind=$2 AND end_at IS NULL AND start_date<>$3
        """, user_id, kind, d)
        try:
            await con.execute("""
                INSERT INTO sessions(user_id, kind, start_at, last_activity, start_date, source)
                VALUES($1,$2,$3,$3,$4,$5)
            """, user_id, kind, t, d, source or "")
        except Exception:
            await con.execute("""
                UPDATE sessions SET last_activity=now()
                WHERE user_id=$1 AND kind=$2 AND end_at IS NULL
            """, user_id, kind)

async def touch_activity(pool, user_id: int, kind: str):
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE sessions SET last_activity=now()
            WHERE user_id=$1 AND kind=$2 AND end_at IS NULL
        """, user_id, kind)

async def close_session(pool, user_id: int, kind: str):
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE sessions SET end_at=now(), last_activity=now()
            WHERE user_id=$1 AND kind=$2 AND end_at IS NULL
        """, user_id, kind)

async def count_open(pool, user_id: int, kind: str) -> int:
    async with pool.acquire() as con:
        return await con.fetchval("""
            SELECT count(*) FROM sessions
            WHERE user_id=$1 AND kind=$2 AND end_at IS NULL
        """, user_id, kind)

async def inc_chat_metrics(pool, user_id: int, msg: Message):
    # فقط گروه اصلی
    d = today_teh()
    is_reply = msg.reply_to_message is not None
    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO chat_metrics(user_id, d, msgs, replies_sent, replies_received)
            VALUES($1,$2,$3,$4,$5)
            ON CONFLICT (user_id, d) DO UPDATE SET
                msgs = chat_metrics.msgs + EXCLUDED.msgs,
                replies_sent = chat_metrics.replies_sent + EXCLUDED.replies_sent,
                replies_received = chat_metrics.replies_received + EXCLUDED.replies_received
        """, user_id, d, 1, (1 if is_reply else 0), 0)
        if is_reply and msg.reply_to_message and msg.reply_to_message.from_user:
            target = msg.reply_to_message.from_user.id
            await con.execute("""
                INSERT INTO chat_metrics(user_id, d, msgs, replies_sent, replies_received)
                VALUES($1,$2,0,0,1)
                ON CONFLICT (user_id, d) DO UPDATE SET
                    replies_received = chat_metrics.replies_received + 1
            """, target, d)

        await con.execute("""
            INSERT INTO candidates_daily(user_id, d, chat_msgs)
            VALUES($1,$2,1)
            ON CONFLICT (user_id, d) DO UPDATE SET
                chat_msgs = candidates_daily.chat_msgs + 1
        """, user_id, d)

# ---------- آمار «فقط گروه اصلی» ----------
def _main_src_like():
    return f"%{src_tag(MAIN_CHAT_ID)}%"

async def admin_today_stats_main(pool, user_id: int):
    d = today_teh()
    pat = _main_src_like()
    async with pool.acquire() as con:
        row = await con.fetchrow("""
        WITH cm AS (
            SELECT COALESCE(SUM(msgs),0) as msgs,
                   COALESCE(SUM(replies_sent),0) as r_sent,
                   COALESCE(SUM(replies_received),0) as r_recv
            FROM chat_metrics WHERE user_id=$1 AND d=$2
        ),
        chat_secs AS (
            SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) AS secs
            FROM sessions
            WHERE user_id=$1 AND kind='chat' AND start_date=$2 AND source LIKE $3
        ),
        call_secs AS (
            SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) AS secs
            FROM sessions
            WHERE user_id=$1 AND kind='call' AND start_date=$2 AND source LIKE $3
        )
        SELECT cm.msgs, cm.r_sent, cm.r_recv, chat_secs.secs as chat_secs, call_secs.secs as call_secs
        FROM cm, chat_secs, call_secs
        """, user_id, d, pat)
        return row

async def admins_overview_today_main(pool):
    d = today_teh()
    pat = _main_src_like()
    async with pool.acquire() as con:
        rows = await con.fetch("""
        WITH u AS (
            SELECT user_id, role, rank, username, first_name, last_name
            FROM users WHERE role <> 'member'
        ),
        chat_secs AS (
            SELECT user_id, COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) as chat_secs
            FROM sessions
            WHERE kind='chat' AND start_date=$1 AND source LIKE $2
            GROUP BY user_id
        ),
        call_secs AS (
            SELECT user_id, COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) as call_secs
            FROM sessions
            WHERE kind='call' AND start_date=$1 AND source LIKE $2
            GROUP BY user_id
        ),
        cm AS (
            SELECT user_id, COALESCE(SUM(msgs),0) as msgs
            FROM chat_metrics WHERE d=$1
            GROUP BY user_id
        )
        SELECT u.*, COALESCE(cm.msgs,0) as msgs,
               COALESCE(chat_secs.chat_secs,0) as chat_secs,
               COALESCE(call_secs.call_secs,0) as call_secs
        FROM u
        LEFT JOIN cm ON cm.user_id=u.user_id
        LEFT JOIN chat_secs ON chat_secs.user_id=u.user_id
        LEFT JOIN call_secs ON call_secs.user_id=u.user_id
        """, d, pat)
        return rows

async def last_30_days_stats_main(pool, user_id: int):
    start_d = today_teh() - timedelta(days=30)
    pat = _main_src_like()
    async with pool.acquire() as con:
        row = await con.fetchrow("""
        WITH cm AS (
            SELECT COALESCE(SUM(msgs),0) msgs,
                   COALESCE(SUM(replies_sent),0) rs,
                   COALESCE(SUM(replies_received),0) rr
            FROM chat_metrics WHERE user_id=$1 AND d >= $2
        ),
        sess AS (
            SELECT kind, COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) secs
            FROM sessions WHERE user_id=$1 AND start_date >= $2 AND source LIKE $3
            GROUP BY kind
        )
        SELECT cm.msgs, cm.rs, cm.rr,
               COALESCE((SELECT secs FROM sess WHERE kind='chat'),0) chat_secs,
               COALESCE((SELECT secs FROM sess WHERE kind='call'),0) call_secs
        """, user_id, start_d, pat)
    return row

# ----------------------------- Keyboards -------------------------------------
def kb_dual(kind_mode: str, user_id: int, show_chat=True, show_call=True):
    b = InlineKeyboardBuilder()
    if show_chat:
        b.button(text=("✅ ثبت ورود چت" if kind_mode=="ci" else "❌ ثبت خروج چت"), callback_data=f"{kind_mode}:chat:{user_id}")
    if show_call:
        b.button(text=("✅ ثبت ورود کال" if kind_mode=="ci" else "❌ ثبت خروج کال"), callback_data=f"{kind_mode}:call:{user_id}")
    if show_chat and show_call:
        b.adjust(2)
    else:
        b.adjust(1)
    return b.as_markup()

def kb_first_msg_dual_checkin(user_id: int):
    return kb_dual("ci", user_id, True, True)

def kb_feedback(target_user_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="👍 راضی", callback_data=f"fb:{target_user_id}:1")
    b.button(text="👎 ناراضی", callback_data=f"fb:{target_user_id}:-1")
    b.adjust(2)
    return b.as_markup()

def kb_admin_panel(role: str, is_owner: bool=False):
    b = InlineKeyboardBuilder()
    b.button(text="📊 آمار من", callback_data="pv:me")
    b.button(text="📈 آمار کلی من", callback_data="pv:me_all")
    b.button(text="✉️ ارتباط با مالک", callback_data="pv:contact_owner")
    b.button(text="📣 پیام به گارد", callback_data="pv:contact_guard")
    b.button(text="🚨 گزارش کاربر", callback_data="pv:report_user")
    if role in {"admin_chat","senior_chat","senior_all"} or is_owner:
        b.button(text="🧑‍💻 لیست ادمین‌های چت", callback_data="pv:list_admins_chat")
        b.button(text="📝 پیام به گروه اصلی", callback_data="pv:send_to_main")
        b.button(text="📮 گزارش به مالک (چت)", callback_data="pv:send_report_owner")
        b.button(text="🚨 گزارش ادمین چت", callback_data="pv:report_admin_chat")
    if role in {"admin_call","senior_call","senior_all"} or is_owner:
        b.button(text="🎙️ لیست ادمین‌های کال", callback_data="pv:list_admins_voice")
        b.button(text="📝 پیام به گروه (کال)", callback_data="pv:send_to_main_voice")
        b.button(text="📮 گزارش به مالک (کال)", callback_data="pv:send_report_owner_voice")
        b.button(text="🚨 گزارش ادمین کال", callback_data="pv:report_admin_voice")
    b.adjust(2)
    return b.as_markup()

# ----------------------------- Guides ----------------------------------------
def help_text_for_role(role: str, is_owner: bool=False) -> str:
    base = [
        "<b>راهنمای سریع</b>",
        "• دکمهٔ «ثبت ورود چت/کال» با اولین پیام امروز در گروه اصلی.",
        "• «ثبت ورود» و «ثبت خروج» یکسان؛ انتخاب نوع با دکمه.",
        "• «ثبت» وضعیت فعلی + آمار امروز را می‌دهد.",
        "• <b>آمار فقط مربوط به گروه اصلی است.</b>",
        "• اگر دکمه‌ها نمی‌آیند: پرایوسی BotFather را Disable کن و شناسهٔ گروه‌ها را با دستور متنی whereami چک کن.",
    ]
    if is_owner or role in {"senior_chat","senior_all"}:
        base += ["", "<b>ابزار ارشد چت</b>", "لیست ادمین‌های چت / پیام به گروه / گزارش به مالک / گزارش ادمین چت"]
    if is_owner or role in {"senior_call","senior_all"}:
        base += ["", "<b>ابزار ارشد کال</b>", "لیست ادمین‌های کال / پیام به گروه (کال) / گزارش به مالک (کال) / گزارش ادمین کال"]
    if is_owner or role == "owner":
        base += ["", "<i>تشخیصی‌ها (متنی): whereami / whoami / health</i>"]
    return "\n".join(base)

def owner_help_text() -> str:
    return "\n".join([
        "<b>👑 راهنمای کامل مالک</b>",
        "",
        "<b>ترفیع/عزل</b>",
        "• <code>ترفیع چت @username|id</code> — ادمین چت",
        "• <code>ترفیع کال @username|id</code> — ادمین کال",
        "• <code>ترفیع ارشدچت @username|id</code> — ارشد چت",
        "• <code>ترفیع ارشدکال @username|id</code> — ارشد کال",
        "• <code>ترفیع ارشدکل @username|id</code> — ارشد کل",
        "• <code>عزل چت|کال|ارشدچت|ارشدکال|ارشدکل @username|id</code>",
        "",
        "<b>آمار (فقط گروه اصلی)</b>",
        "• <code>آمار چت الان</code> — زمان چت + پیام‌های امروز",
        "• <code>آمار کال الان</code> — زمان کال امروز",
        "• <code>آمار</code> — تعداد کاربران فعال امروز",
        "• <code>آمار کلی کاربر id</code> — ۳۰ روز اخیر",
        "",
        "<b>تگ گروهی (ریپلای روی پیام یا مستقل)</b>",
        "• <code>تگ چت</code> / <code>تگ کال</code> / <code>تگ همه</code>",
        "",
        "<b>تشخیصی (متنی)</b>",
        "• <code>whereami</code> — هرجا (فقط مالک/ادمین‌ها)",
        "• <code>whoami</code> — پی‌وی",
        "• <code>health</code> — پی‌وی فقط مالک",
        "",
        "<b>ممنوع/آزاد</b>",
        "• ریپلای کنید یا @username یا id بدهید: <code>ممنوع</code> / <code>آزاد</code>"
    ])

# ----------------------------- Bot Init --------------------------------------
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TEHRAN)
pool: asyncpg.Pool = None
tclient = None

# ----------------------------- Helpers (Resolvers) ---------------------------
async def resolve_user_identifier(msg: Message, ident: str | None) -> int | None:
    """
    سعی می‌کند هدف را از ۳ راه پیدا کند:
    1) ریپلای به پیام → from_user.id
    2) آیدی عددی
    3) @username → اول Bot API (get_chat) سپس دیتابیس users
    """
    # 1) reply
    if msg.reply_to_message and msg.reply_to_message.from_user:
        return msg.reply_to_message.from_user.id

    # 2) numeric id
    if ident and ident.isdigit():
        return int(ident)

    # 3) username
    if ident and ident.startswith("@"):
        uname = ident[1:]
        # try Bot API
        try:
            ch = await bot.get_chat(ident)
            if getattr(ch, "id", None):
                return ch.id
        except Exception:
            pass
        # fallback: DB (case-insensitive)
        async with pool.acquire() as con:
            uid = await con.fetchval(
                "SELECT user_id FROM users WHERE lower(username)=lower($1) LIMIT 1",
                uname
            )
            if uid:
                return int(uid)
    return None

# ----------------------------- Help Handlers ---------------------------------
@dp.message(F.text.regexp(r"^(?:راهنما|help)$"), F.chat.type == ChatType.PRIVATE)
async def help_pv(msg: Message):
    role = await get_role(pool, msg.from_user.id)
    is_owner = (msg.from_user.id == OWNER_ID)
    if is_owner:
        return await msg.answer(owner_help_text())
    await msg.answer(help_text_for_role(role, is_owner))

@dp.message(F.text.regexp(r"^(?:راهنما|help)$"))
async def help_anywhere(msg: Message):
    if msg.from_user.id == OWNER_ID:
        return await msg.reply(owner_help_text())
    role = await get_role(pool, msg.from_user.id)
    await msg.reply(help_text_for_role(role, is_owner=False))

# ---------------------------- Diagnostics (TEXT) ------------------------------
@dp.message(F.text.regexp(r"^(?:whereami|کجا(?:ی)?(?: هستم)?|کجا هستیم)$"))
async def whereami_text(msg: Message):
    who = msg.from_user
    role = await get_role(pool, who.id)
    if who.id != OWNER_ID and not is_admin_role(role):
        return await msg.reply("این دستور برای مالک/ادمین‌هاست.")
    title = msg.chat.title or ""
    await msg.reply(
        f"🛰️ <b>whereami</b>\n"
        f"chat_id: <code>{msg.chat.id}</code>\n"
        f"type: <code>{msg.chat.type}</code>\n"
        f"title: <code>{title}</code>"
    )

@dp.message(F.chat.type == ChatType.PRIVATE, F.text.regexp(r"^(?:whoami|آیدی من|ایدی من)$"))
async def whoami_text(msg: Message):
    await msg.reply(f"🆔 آیدی شما: <code>{msg.from_user.id}</code>")

@dp.message(F.chat.type == ChatType.PRIVATE, F.text.regexp(r"^(?:health|سلامت|وضعیت)$"))
async def health_text(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return await msg.reply("فقط مالک.")
    db_ok = False
    try:
        async with pool.acquire() as con:
            _ = await con.fetchval("SELECT 1")
            db_ok = True
    except Exception as e:
        db_err = str(e)
    lines = [
        f"💚 <b>Souls Guard — Health</b> v{APP_VERSION}",
        f"uptime: {uptime_str()}",
        f"DB: {'OK' if db_ok else 'FAIL'}",
        f"MAIN_CHAT_ID: <code>{MAIN_CHAT_ID}</code>",
        f"GUARD_CHAT_ID: <code>{GUARD_CHAT_ID}</code>",
        f"TEHRAN now: <code>{now_teh().isoformat()}</code>",
        "Jobs: autoclose(chat/call) each minute, daily rollover 00:00 Tehran",
        "⚠️ اگر در گروه اصلی پیام می‌دهید و دکمه‌ها نمی‌آیند: پرایوسی BotFather را Disable کنید و ربات را دوباره به گروه اضافه کنید.",
    ]
    if not db_ok:
        lines.append(f"DB error: <code>{db_err}</code>")
    await msg.reply("\n".join(lines))

# ---------------------------- Startup ----------------------------------------
async def on_startup():
    global pool, tclient
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as con:
        for stmt in [s.strip() for s in SCHEMA_SQL.split(";") if s.strip()]:
            await con.execute(stmt + ";")
        await con.execute("""
            INSERT INTO groups (group_type, chat_id, title)
            VALUES ('main', $1, 'souls')
            ON CONFLICT (group_type) DO UPDATE SET chat_id=EXCLUDED.chat_id, title=EXCLUDED.title
        """, MAIN_CHAT_ID)
        await con.execute("""
            INSERT INTO groups (group_type, chat_id, title)
            VALUES ('guard', $1, 'souls guard')
            ON CONFLICT (group_type) DO UPDATE SET chat_id=EXCLUDED.chat_id, title=EXCLUDED.title
        """, GUARD_CHAT_ID)

    if ENABLE_TELETHON and API_ID and API_HASH and TELETHON_SESSION:
        tclient = TelegramClient(StringSession(TELETHON_SESSION), API_ID, API_HASH)
        await tclient.start()
        log.info("Telethon userbot started.")

    scheduler.add_job(job_autoclose_inactive_chat, CronTrigger.from_crontab("*/1 * * * *"))
    scheduler.add_job(job_autoclose_inactive_call_fallback, CronTrigger.from_crontab("*/1 * * * *"))
    scheduler.add_job(job_daily_rollover_main_only, CronTrigger(hour=0, minute=0))
    scheduler.start()
    log.info("Scheduler started.")
    try:
        await bot.send_message(OWNER_ID, f"✅ Bot started v{APP_VERSION} — tz={TEHRAN.key}, uptime {uptime_str()}")
    except Exception:
        pass

dp.startup.register(on_startup)

# --------------------- Jobs (auto-close & daily report) ----------------------
async def job_autoclose_inactive_chat():
    try:
        async with pool.acquire() as con:
            rows = await con.fetch("""
                SELECT s.user_id
                FROM sessions s
                JOIN users u ON u.user_id=s.user_id
                WHERE s.kind='chat' AND s.end_at IS NULL
                  AND now() - s.last_activity > INTERVAL '10 minutes'
            """)
        for r in rows:
            await close_session(pool, r["user_id"], "chat")
            try:
                mention = f"<a href=\"tg://user?id={r['user_id']}\">{r['user_id']}</a>"
                text = f"⏹️ خروج خودکار چت برای {mention} پس از ۱۰ دقیقه بی‌فعالی ثبت شد."
                await bot.send_message(GUARD_CHAT_ID, text)
                await bot.send_message(OWNER_ID, text)
            except Exception:
                pass
    except Exception as e:
        log.error(f"job_autoclose_inactive_chat: {e}")

async def job_autoclose_inactive_call_fallback():
    if ENABLE_TELETHON:
        return
    now = now_teh()
    to_close = []
    for uid, last in list(CALL_HEARTBEATS.items()):
        if (now - last) > timedelta(minutes=10):
            to_close.append(uid)
    for uid in to_close:
        try:
            await close_session(pool, uid, "call")
            mention = f"<a href=\"tg://user?id={uid}\">{uid}</a>"
            txt = f"⏹️ خروج خودکار کال برای {mention} پس از ۱۰ دقیقه ثبت شد."
            await bot.send_message(GUARD_CHAT_ID, txt)
            await bot.send_message(OWNER_ID, txt)
        except Exception:
            pass
        CALL_HEARTBEATS.pop(uid, None)

async def job_daily_rollover_main_only():
    try:
        rows = await admins_overview_today_main(pool)
        if not rows:
            return
        lines = ["📊 <b>آمار امروز ادمین‌ها — فقط گروه اصلی</b>\n(از ۰۰:۰۰ تا اکنون به وقت تهران)\n"]
        rows_sorted = sorted(rows, key=lambda r: (ROLE_ORDER.get(r["role"], 999), -r["msgs"], -r["chat_secs"], -r["call_secs"]))
        for r in rows_sorted:
            name = r["first_name"] or ""
            un = f"@{r['username']}" if r["username"] else ""
            rt = role_title(r["role"])
            lines.append(
                f"{rt} — <a href=\"tg://user?id={r['user_id']}\">{name or r['user_id']}</a> {un}\n"
                f"• پیام‌ها(اصلی): <b>{r['msgs']}</b> | چت(اصلی): <b>{pretty_td(r['chat_secs'])}</b> | کال(اصلی): <b>{pretty_td(r['call_secs'])}</b>"
            )
        text = "\n".join(lines)
        # ارسال به پی‌وی مالک و گارد
        await bot.send_message(OWNER_ID, text)
        await bot.send_message(GUARD_CHAT_ID, text)
    except Exception as e:
        log.error(f"job_daily_rollover_main_only: {e}")

# ------------------------------ Handlers -------------------------------------

@dp.my_chat_member()
async def my_member_updates(ev: ChatMemberUpdated):
    try:
        chat = ev.chat
        new = ev.new_chat_member
        old = ev.old_chat_member
        await bot.send_message(
            OWNER_ID,
            f"ℹ️ <b>my_chat_member</b>\n"
            f"chat_id: <code>{chat.id}</code> ({chat.type})\n"
            f"status: <code>{old.status} ➜ {new.status}</code>\n"
            f"is_admin: <code>{getattr(new, 'is_chat_admin', False)}</code>"
        )
    except Exception:
        pass

# شمارش پیام‌ها: فقط گروه اصلی (برای آمار)
@dp.message((F.chat.id == MAIN_CHAT_ID) | (F.chat.id == GUARD_CHAT_ID), F.from_user)
async def any_group_common(msg: Message):
    u = msg.from_user
    await ensure_user(pool, u)
    async with pool.acquire() as con:
        banned = await con.fetchval("SELECT 1 FROM bans WHERE user_id=$1", u.id)
    if banned:
        try:
            await bot.delete_message(msg.chat.id, msg.message_id)
        except Exception:
            pass
        return
    if msg.chat.id == MAIN_CHAT_ID:
        await inc_chat_metrics(pool, u.id, msg)

# /start در پی‌وی
@dp.message(CommandStart(), F.chat.type == ChatType.PRIVATE)
async def start_pv(msg: Message):
    await ensure_user(pool, msg.from_user)
    role = await get_role(pool, msg.from_user.id)
    if msg.from_user.id == OWNER_ID and role != "owner":
        await set_role(pool, msg.from_user.id, "owner")
        role = "owner"
    if is_admin_role(role) or msg.from_user.id == OWNER_ID:
        await msg.answer(
            "به پنل گارد سولز خوش آمدید.\nاز دکمه‌ها استفاده کنید:",
            reply_markup=kb_admin_panel(role, is_owner=(msg.from_user.id==OWNER_ID))
        )
    else:
        await msg.answer(
            "این ربات مخصوص ادمین‌های گارد سولز است.\n"
            "برای ارتباط با مالک از ربات @soulsownerbot استفاده کنید.",
            reply_markup=ReplyKeyboardRemove()
        )

# معادل متنی شروع پنل در پی‌وی
@dp.message(F.chat.type == ChatType.PRIVATE, F.text.regexp(r"^(?:سلام|شروع|پنل|panel|menu|منو)$"))
async def pv_open_panel(msg: Message):
    await ensure_user(pool, msg.from_user)
    role = await get_role(pool, msg.from_user.id)
    await msg.answer("پنل شما:", reply_markup=kb_admin_panel(role, is_owner=(msg.from_user.id==OWNER_ID)))

# گروه اصلی: اولین پیام امروز → دکمه‌های ورود چت/کال
@dp.message(F.chat.id == MAIN_CHAT_ID, F.from_user)
async def main_group_prompt_first(msg: Message):
    u = msg.from_user
    await ensure_user(pool, u)
    role = await get_role(pool, u.id)
    if role in (ALLOWED_CHAT_ROLES | ALLOWED_VOICE_ROLES) or u.id == OWNER_ID:
        if await count_open(pool, u.id, "chat") == 0:
            d = today_teh()
            async with pool.acquire() as con:
                shown = await con.fetchval(
                    "SELECT 1 FROM first_prompt_main WHERE user_id=$1 AND d=$2",
                    u.id, d
                )
                if not shown:
                    await msg.reply(
                        f"اولین پیام امروز ثبت شد. {u.first_name} عزیز، یکی از گزینه‌ها را بزن:",
                        reply_markup=kb_first_msg_dual_checkin(u.id)
                    )
                    await con.execute(
                        "INSERT INTO first_prompt_main(user_id, d) VALUES($1,$2) ON CONFLICT DO NOTHING",
                        u.id, d
                    )

# میان‌برهای یکسان «ثبت ورود / ثبت خروج / ثبت» در هر دو گروه (متنی)
@dp.message((F.chat.id == MAIN_CHAT_ID) | (F.chat.id == GUARD_CHAT_ID), F.text)
async def unified_shortcuts(msg: Message):
    u = msg.from_user
    role = await get_role(pool, u.id)
    text = re.sub(r"\s+", " ", (msg.text or "").strip().lower())
    if text not in {"ثبت ورود", "ثبت خروج", "ثبت"}:
        return
    if not (role in (ALLOWED_CHAT_ROLES | ALLOWED_VOICE_ROLES) or u.id == OWNER_ID):
        return await msg.reply("این دستور مخصوص ادمین‌ها و ارشدهاست.")

    if text == "ثبت":
        st = await admin_today_stats_main(pool, u.id)
        open_chat = await count_open(pool, u.id, "chat") > 0
        open_call = await count_open(pool, u.id, "call") > 0
        status = []
        status.append(f"چت: {'✅ باز' if open_chat else '⏹️ بسته'}")
        status.append(f"کال: {'✅ باز' if open_call else '⏹️ بسته'}")
        txt = (f"📍 <b>وضعیت ثبت (اصلی)</b>\n" +
               " — ".join(status) + "\n\n" +
               f"📊 <b>آمار امروز (فقط اصلی)</b>\n"
               f"پیام‌ها: <b>{st['msgs']}</b>\n"
               f"ریپلای‌ها (ارسال/دریافت): <b>{st['r_sent']}/{st['r_recv']}</b>\n"
               f"زمان چت: <b>{pretty_td(st['chat_secs'])}</b>\n"
               f"زمان کال: <b>{pretty_td(st['call_secs'])}</b>")
        return await msg.reply(txt, reply_markup=kb_dual("ci", u.id, True, True))

    if text == "ثبت ورود":
        return await msg.reply("نوع ثبت ورود را انتخاب کنید:", reply_markup=kb_dual("ci", u.id, True, True))

    if text == "ثبت خروج":
        open_chat = await count_open(pool, u.id, "chat") > 0
        open_call = await count_open(pool, u.id, "call") > 0
        if open_chat ^ open_call:
            kind = "chat" if open_chat else "call"
            await close_session(pool, u.id, kind)
            if kind == "call":
                CALL_HEARTBEATS.pop(u.id, None)
            await msg.reply(f"⏹️ خروج {('چت' if kind=='chat' else 'کال')} ثبت شد.")
            mention = f"<a href=\"tg://user?id={u.id}\">{u.first_name}</a>"
            await bot.send_message(GUARD_CHAT_ID, f"⏹️ {mention} خروج {('چت' if kind=='chat' else 'کال')} زد.")
            await bot.send_message(OWNER_ID, f"⏹️ {mention} خروج {('چت' if kind=='chat' else 'کال')} زد.")
        else:
            if not open_chat and not open_call:
                await msg.reply("سشنی باز نیست. اگر لازم است یکی را انتخاب کنید:", reply_markup=kb_dual("co", u.id, True, True))
            else:
                await msg.reply("کدام را می‌خواهید خارج شوید؟", reply_markup=kb_dual("co", u.id, True, True))

# کال: دکمهٔ اینلاین با متن «کال»
@dp.message(F.chat.id == MAIN_CHAT_ID, F.text.regexp(r"^کال$"))
async def main_group_call_help(msg: Message):
    role = await get_role(pool, msg.from_user.id)
    if (role not in (ALLOWED_CHAT_ROLES | ALLOWED_VOICE_ROLES)) and msg.from_user.id != OWNER_ID:
        return await msg.reply("این بخش مخصوص ادمین‌ها/ارشدها/مالک است.")
    await msg.reply("برای کال یکی از گزینه‌ها را بزن:", reply_markup=kb_dual("ci", msg.from_user.id, False, True))

# کال‌بک‌های ورود/خروج (چت/کال)
@dp.callback_query(F.data.regexp(r"^(ci|co):(chat|call):(\d+)$"))
async def cb_checkin_out(cb: CallbackQuery):
    action, kind, uid = cb.data.split(":")
    uid = int(uid)
    if cb.from_user.id != uid and cb.from_user.id != OWNER_ID:
        return await cb.answer("این دکمه مخصوص همان کاربر/مالک است.", show_alert=True)

    await ensure_user(pool, cb.from_user)
    role = await get_role(pool, cb.from_user.id)
    if kind == "chat":
        if not (role in ALLOWED_CHAT_ROLES or cb.from_user.id == OWNER_ID):
            return await cb.answer("اجازهٔ چت ندارید.", show_alert=True)
    else:
        if not (role in ALLOWED_VOICE_ROLES or cb.from_user.id == OWNER_ID):
            return await cb.answer("اجازهٔ کال ندارید.", show_alert=True)

    src = f"inline:{src_tag(cb.message.chat.id)}"

    if action == "ci":
        if await count_open(pool, uid, kind) > 0:
            return await cb.answer("سشن باز داری.", show_alert=True)
        await open_session(pool, uid, kind, source=src)
        if kind == "call":
            CALL_HEARTBEATS[uid] = now_teh()
        await cb.message.edit_text(f"✅ ثبت ورود {('چت' if kind=='chat' else 'کال')} انجام شد.")
        mention = f"<a href=\"tg://user?id={uid}\">{cb.from_user.first_name}</a>"
        await bot.send_message(GUARD_CHAT_ID, f"✅ {mention} ورود {('چت' if kind=='chat' else 'کال')} زد.")
        await bot.send_message(OWNER_ID, f"✅ {mention} ورود {('چت' if kind=='chat' else 'کال')} زد.")
    else:
        await close_session(pool, uid, kind)
        if kind == "call":
            CALL_HEARTBEATS.pop(uid, None)
        await cb.message.edit_text(f"⏹️ خروج {('چت' if kind=='chat' else 'کال')} ثبت شد.")
        mention = f"<a href=\"tg://user?id={uid}\">{cb.from_user.first_name}</a>"
        await bot.send_message(GUARD_CHAT_ID, f"⏹️ {mention} خروج {('چت' if kind=='chat' else 'کال')} زد.")
        await bot.send_message(OWNER_ID, f"⏹️ {mention} خروج {('چت' if kind=='chat' else 'کال')} زد.")
    await cb.answer()

# ----------------- پنل پیوی و ابزارها -----------------
@dp.callback_query(F.data.startswith("pv:"))
async def pv_buttons(cb: CallbackQuery):
    await ensure_user(pool, cb.from_user)
    role = await get_role(pool, cb.from_user.id)
    is_owner = (cb.from_user.id == OWNER_ID)

    if cb.data == "pv:me":
        st = await admin_today_stats_main(pool, cb.from_user.id)
        if st:
            open_chat = await count_open(pool, cb.from_user.id, "chat") > 0
            open_call = await count_open(pool, cb.from_user.id, "call") > 0
            status = f"وضعیت: چت {'✅' if open_chat else '⏹️'} | کال {'✅' if open_call else '⏹️'}\n"
            txt = (f"📊 <b>آمار امروز (فقط اصلی)</b>\n{status}"
                   f"پیام‌ها: <b>{st['msgs']}</b>\n"
                   f"ریپلای‌ها (ارسال/دریافت): <b>{st['r_sent']}/{st['r_recv']}</b>\n"
                   f"زمان چت: <b>{pretty_td(st['chat_secs'])}</b>\n"
                   f"زمان کال: <b>{pretty_td(st['call_secs'])}</b>\n")
            await cb.message.edit_text(txt, reply_markup=kb_admin_panel(role, is_owner))
        return await cb.answer()

    if cb.data == "pv:me_all":
        st = await last_30_days_stats_main(pool, cb.from_user.id)
        txt = (f"📈 <b>۳۰ روز اخیر (فقط اصلی)</b>\n"
               f"پیام‌ها: <b>{st['msgs']}</b>\n"
               f"ریپلای‌ها (ارسال/دریافت): <b>{st['rs']}/{st['rr']}</b>\n"
               f"چت: <b>{pretty_td(st['chat_secs'])}</b> | کال: <b>{pretty_td(st['call_secs'])}</b>")
        await cb.message.edit_text(txt, reply_markup=kb_admin_panel(role, is_owner))
        return await cb.answer()

    if cb.data == "pv:contact_owner":
        PENDING_CONTACT_OWNER.add(cb.from_user.id)
        await cb.message.edit_text("پیام‌تان به مالک را ارسال کنید (لغو: لغو / cancel)")
        return await cb.answer()

    if cb.data == "pv:contact_guard":
        PENDING_CONTACT_GUARD.add(cb.from_user.id)
        await cb.message.edit_text("پیام شما به گروه گارد ارسال می‌شود: الان متن را بفرستید. (لغو: لغو / cancel)")
        return await cb.answer()

    if cb.data == "pv:report_user":
        PENDING_REPORT[cb.from_user.id] = {"type": "member"}
        await cb.message.edit_text("آیدی عددی یا یوزرنیم کاربر را بفرستید.")
        return await cb.answer()

    if cb.data == "pv:list_admins_chat":
        if not (role in {"admin_chat","senior_chat","senior_all"} or is_owner):
            return await cb.answer("اجازه دسترسی ندارید.", show_alert=True)
        async with pool.acquire() as con:
            rows = await con.fetch("SELECT user_id, username, first_name, role FROM users WHERE role IN ('admin_chat','senior_chat','senior_all','owner') ORDER BY role")
        lines = ["🧑‍💻 ادمین‌های چت:"]
        for r in rows:
            lines.append(f"• {role_title(r['role'])}: <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a> @{r['username'] or ''}")
        await cb.message.edit_text("\n".join(lines))
        return await cb.answer()

    if cb.data == "pv:list_admins_voice":
        if not (role in {"admin_call","senior_call","senior_all"} or is_owner):
            return await cb.answer("اجازه دسترسی ندارید.", show_alert=True)
        async with pool.acquire() as con:
            rows = await con.fetch("SELECT user_id, username, first_name, role FROM users WHERE role IN ('admin_call','senior_call','senior_all','owner') ORDER BY role")
        lines = ["🎙️ ادمین‌های کال:"]
        for r in rows:
            lines.append(f"• {role_title(r['role'])}: <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a> @{r['username'] or ''}")
        await cb.message.edit_text("\n".join(lines))
        return await cb.answer()

    if cb.data in {"pv:send_to_main","pv:send_report_owner","pv:report_admin_chat"}:
        if not (role in {"admin_chat","senior_chat","senior_all"} or is_owner):
            return await cb.answer("اجازه دسترسی ندارید.", show_alert=True)
        await cb.message.edit_text("متن خود را ارسال کنید. (لغو: لغو / cancel)")
        PENDING_REPORT[cb.from_user.id] = {"type": cb.data}
        return await cb.answer()

    if cb.data in {"pv:send_to_main_voice","pv:send_report_owner_voice","pv:report_admin_voice"}:
        if not (role in {"admin_call","senior_call","senior_all"} or is_owner):
            return await cb.answer("اجازه دسترسی ندارید.", show_alert=True)
        await cb.message.edit_text("متن خود را ارسال کنید. (لغو: لغو / cancel)")
        PENDING_REPORT[cb.from_user.id] = {"type": cb.data}
        return await cb.answer()

    return await cb.answer()

# دریافت متن‌های پس از دکمه‌های پیوی + fallbackهای متنی
@dp.message(F.chat.type == ChatType.PRIVATE)
async def pv_text_flow(msg: Message):
    uid = msg.from_user.id
    role = await get_role(pool, uid)

    if (msg.text or "").strip().lower() in {"لغو", "cancel", "/cancel"}:
        PENDING_CONTACT_GUARD.discard(uid)
        PENDING_CONTACT_OWNER.discard(uid)
        PENDING_REPORT.pop(uid, None)
        return await msg.reply("لغو شد.", reply_markup=kb_admin_panel(role, is_owner=(uid==OWNER_ID)))

    t = (msg.text or "").strip().lower()

    if t in {"پنل","panel","menu","منو"}:
        return await msg.answer("پنل شما:", reply_markup=kb_admin_panel(role, is_owner=(uid==OWNER_ID)))

    if t in {"آمار من","stats me"}:
        st = await admin_today_stats_main(pool, uid)
        open_chat = await count_open(pool, uid, "chat") > 0
        open_call = await count_open(pool, uid, "call") > 0
        status = f"وضعیت: چت {'✅' if open_chat else '⏹️'} | کال {'✅' if open_call else '⏹️'}\n"
        txt = (f"📊 <b>آمار امروز (فقط اصلی)</b>\n{status}"
               f"پیام‌ها: <b>{st['msgs']}</b>\n"
               f"ریپلای‌ها (ارسال/دریافت): <b>{st['r_sent']}/{st['r_recv']}</b>\n"
               f"زمان چت: <b>{pretty_td(st['chat_secs'])}</b>\n"
               f"زمان کال: <b>{pretty_td(st['call_secs'])}</b>\n")
        return await msg.answer(txt, reply_markup=kb_dual("ci", uid, True, True))

    if t in {"آمار کلی من","stats all"}:
        st = await last_30_days_stats_main(pool, uid)
        txt = (f"📈 <b>۳۰ روز اخیر (فقط اصلی)</b>\n"
               f"پیام‌ها: <b>{st['msgs']}</b>\n"
               f"ریپلای‌ها (ارسال/دریافت): <b>{st['rs']}/{st['rr']}</b>\n"
               f"چت: <b>{pretty_td(st['chat_secs'])}</b> | کال: <b>{pretty_td(st['call_secs'])}</b>")
        return await msg.answer(txt)

    if t in {"ارتباط با مالک","contact owner"}:
        PENDING_CONTACT_OWNER.add(uid)
        return await msg.answer("پیام‌تان به مالک را ارسال کنید (لغو: لغو / cancel)")

    if t in {"پیام به گارد","contact guard"}:
        PENDING_CONTACT_GUARD.add(uid)
        return await msg.answer("پیام شما به گروه گارد ارسال می‌شود: الان متن را بفرستید. (لغو: لغو / cancel)")

    if t in {"گزارش کاربر","report user"}:
        PENDING_REPORT[uid] = {"type": "member"}
        return await msg.answer("آیدی عددی یا یوزرنیم کاربر را بفرستید.")

    if uid in PENDING_CONTACT_OWNER:
        PENDING_CONTACT_OWNER.discard(uid)
        await bot.send_message(OWNER_ID, f"📩 پیام از <a href=\"tg://user?id={uid}\">{msg.from_user.first_name}</a>:\n{msg.text}")
        return await msg.reply("به مالک ارسال شد ✅")

    if uid in PENDING_CONTACT_GUARD:
        PENDING_CONTACT_GUARD.discard(uid)
        await bot.send_message(GUARD_CHAT_ID, f"📣 پیام از <a href=\"tg://user?id={uid}\">{msg.from_user.first_name}</a>:\n{msg.text}")
        return await msg.reply("به گارد ارسال شد ✅")

    if uid in PENDING_REPORT:
        ctx = PENDING_REPORT.pop(uid)
        if ctx["type"] == "member":
            await bot.send_message(OWNER_ID, f"🚨 گزارش از <a href=\"tg://user?id={uid}\">{msg.from_user.first_name}</a>:\n{msg.text}")
            return await msg.reply("گزارش به مالک ارسال شد ✅")

# ------------------ درخواست ادمینی برای اعضای عادی ------------------
def kb_apply_admin():
    b = InlineKeyboardBuilder()
    b.button(text="📨 درخواست ادمینی چت", callback_data="rq:chat")
    b.button(text="🎙️ درخواست ادمینی کال", callback_data="rq:call")
    b.adjust(1,1)
    return b.as_markup()

@dp.message((F.chat.id == MAIN_CHAT_ID) | (F.chat.id == GUARD_CHAT_ID), F.text.regexp(r"^درخواست ادمینی$"))
async def request_admin_command(msg: Message):
    role = await get_role(pool, msg.from_user.id)
    if role != "member":
        return await msg.reply("شما هم‌اکنون عضو گارد هستید. در صورت نیاز با ارشدها/مالک هماهنگ کنید.")
    await msg.reply("نوع درخواست ادمینی را انتخاب کنید:", reply_markup=kb_apply_admin())

async def get_user_last_days_stats(pool, user_id: int, days: int = 7):
    start_d = today_teh() - timedelta(days=days)
    pat = _main_src_like()
    async with pool.acquire() as con:
        row = await con.fetchrow("""
        WITH cm AS (
            SELECT COALESCE(SUM(msgs),0) AS chat_msgs
            FROM chat_metrics
            WHERE user_id=$1 AND d >= $2
        ),
        chat_secs AS (
            SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at,now()) - start_at)))::INT,0) AS secs
            FROM sessions WHERE user_id=$1 AND kind='chat' AND start_date >= $2 AND source LIKE $3
        ),
        call_secs AS (
            SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at,now()) - start_at)))::INT,0) AS secs
            FROM sessions WHERE user_id=$1 AND kind='call' AND start_date >= $2 AND source LIKE $3
        )
        SELECT cm.chat_msgs, chat_secs.secs AS chat_secs, call_secs.secs AS call_secs
        FROM cm, chat_secs, call_secs
        """, user_id, start_d, pat)
    return row or {"chat_msgs":0, "chat_secs":0, "call_secs":0}

@dp.callback_query(F.data.regexp(r"^rq:(chat|call)$"))
async def request_admin_cb(cb: CallbackQuery):
    kind = cb.data.split(":")[1]
    uid = cb.from_user.id
    role = await get_role(pool, uid)
    if role != "member":
        await cb.answer("شما هم‌اکنون عضو گارد هستید.", show_alert=True); return
    st7 = await get_user_last_days_stats(pool, uid, days=7)
    name = cb.from_user.first_name or str(uid)
    un = f"@{cb.from_user.username}" if cb.from_user.username else ""
    text = (
        f"📝 <b>درخواست ادمینی {'چت' if kind=='chat' else 'کال'}</b>\n"
        f"درخواست‌دهنده: <a href=\"tg://user?id={uid}\">{name}</a> {un}\n"
        f"• پیام‌ها (۷روز/اصلی): <b>{st7['chat_msgs']}</b>\n"
        f"• زمان چت (۷روز/اصلی): <b>{pretty_td(st7['chat_secs'])}</b>\n"
        f"• زمان کال (۷روز/اصلی): <b>{pretty_td(st7['call_secs'])}</b>\n"
    )
    await bot.send_message(GUARD_CHAT_ID, text)
    await bot.send_message(OWNER_ID, text)
    await cb.message.edit_text("✅ درخواست شما ارسال شد. نتیجه از طرف مدیریت اعلام می‌شود.")
    await cb.answer()

# ------------------ تگ گروهی (همه‌جا) ------------------
async def fetch_role_user_ids(pool, roles):
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT user_id, first_name FROM users WHERE role = ANY($1::text[])", list(roles))
    return [(r["user_id"], r["first_name"] or str(r["user_id"])) for r in rows]

def mentions_from_list(items, limit=50):
    out = []
    for uid, name in items[:limit]:
        out.append(f"<a href=\"tg://user?id={uid}\">{name}</a>")
    return " ".join(out)

@dp.message(F.text.regexp(r"^تگ\s*(چت|کال|همه)$"))
async def tag_commands(msg: Message):
    who = msg.from_user
    role = await get_role(pool, who.id)
    target = (msg.text or "").strip().split()[-1]

    if who.id != OWNER_ID and role not in {"senior_all","senior_chat","senior_call"}:
        return await msg.reply("این دستور فقط برای مالک و ارشدهاست.")
    if target == "همه" and (who.id != OWNER_ID and role != "senior_all"):
        return await msg.reply("«تگ همه» فقط برای مالک یا ارشدکل مجاز است.")

    tag_ids = []
    if target == "چت":
        ids = await fetch_role_user_ids(pool, {"admin_chat","senior_chat","senior_all","owner"})
        tag_ids.extend(ids)
    elif target == "کال":
        ids = await fetch_role_user_ids(pool, {"admin_call","senior_call","senior_all","owner"})
        tag_ids.extend(ids)
    else:
        ids1 = await fetch_role_user_ids(pool, {"admin_chat","senior_chat"})
        ids2 = await fetch_role_user_ids(pool, {"admin_call","senior_call"})
        ids3 = await fetch_role_user_ids(pool, {"senior_all","owner"})
        seen = set(); merged = []
        for lst in (ids1+ids2+ids3):
            if lst[0] not in seen:
                seen.add(lst[0]); merged.append(lst)
        tag_ids = merged

    if not tag_ids:
        return await msg.reply("کسی برای تگ یافت نشد.")
    tags = mentions_from_list(tag_ids, limit=50)
    if msg.reply_to_message:
        await msg.reply_to_message.reply(f"🔔 {tags}")
    else:
        await msg.reply(f"🔔 {tags}")

# ------------------ رأی مالک ------------------
@dp.callback_query(F.data.regexp(r"^fb:(\d+):(-?1)$"))
async def feedback_cb(cb: CallbackQuery):
    target, score = cb.data.split(":")[1:]
    target = int(target); score = int(score)
    if cb.from_user.id != OWNER_ID:
        return await cb.answer("فقط مالک می‌تواند رأی دهد.", show_alert=True)
    async with pool.acquire() as con:
        await con.execute("INSERT INTO feedback(target_user_id, giver_user_id, d, score) VALUES($1,$2,$3,$4)",
                          target, OWNER_ID, today_teh(), score)
    await cb.answer("ثبت شد.", show_alert=False)
    await cb.message.edit_reply_markup(reply_markup=None)

# ----------------------- دستورهای متنی مالک (بدون /) -----------------------
OWNER_CMD_PATTERNS = [
    (r"^(ترفیع|عزل)\s+(چت|کال|ارشدچت|ارشدکال|ارشدکل)\s+(@\w+|\d+)?$", "promote_demote"),
    (r"^آمار\s*چت\s*الان$", "stats_chat_now"),
    (r"^آمار\s*کال\s*الان$", "stats_call_now"),
    (r"^آمار\s*$", "stats_active"),
    (r"^ممنوع(?:\s+(@\w+|\d+))?$", "ban_user"),
    (r"^آزاد(?:\s+(@\w+|\d+))?$", "unban_user"),
    (r"^اتک\s*بک\s+(.+)$", "attack_back"),
    (r"^تایتل\s*کال\s+(.+)$", "call_title"),
    (r"^آمار\s*کلی\s*کاربر\s+(\d+)$", "user_month")
]

ROLE_MAP = {
    "چت": "admin_chat",
    "کال": "admin_call",
    "ارشدچت": "senior_chat",
    "ارشدکال": "senior_call",
    "ارشدکل": "senior_all"
}

@dp.message(F.from_user.id == OWNER_ID)
async def owner_text_commands(msg: Message):
    text_raw = (msg.text or "").strip()
    text = re.sub(r"\s+", " ", text_raw)
    if re.fullmatch(r"(?:راهنما|help)", text):
        return await msg.reply(owner_help_text())

    for pat, name in OWNER_CMD_PATTERNS:
        m = re.match(pat, text)
        if not m: 
            continue

        # -------- ترفیع/عزل با ریپلای/ID/@username --------
        if name == "promote_demote":
            act, kind, ident = m.groups()
            target_id = await resolve_user_identifier(msg, ident)
            if not target_id:
                return await msg.reply("❗ لطفاً روی پیام فرد ریپلای کنید یا آیدی عددی/یوزرنیم معتبر بدهید.")
            role_key = ROLE_MAP[kind]
            if act == "ترفیع":
                ok = await set_role(pool, target_id, role_key)
                if not ok:
                    await msg.reply("نقش نامعتبر.")
                    return
                await msg.reply(f"✅ {target_id} به {role_title(role_key)} ترفیع یافت.")
            else:
                await set_role(pool, target_id, "member")
                await msg.reply(f"✅ {target_id} عزل شد.")
            return

        # -------- آمار لحظه‌ای (ارسال به PV مالک و گارد) --------
        elif name == "stats_chat_now":
            rows = await admins_overview_today_main(pool)
            lines = ["📊 آمار چت تا این لحظه — فقط اصلی:"]
            for r in sorted(rows, key=lambda r: ROLE_ORDER.get(r["role"], 99)):
                lines.append(f"{role_title(r['role'])} — <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a>: چت {pretty_td(r['chat_secs'])} | پیام {r['msgs']}")
            text_stats = "\n".join(lines)
            await bot.send_message(OWNER_ID, text_stats)
            await bot.send_message(GUARD_CHAT_ID, text_stats)
            await msg.reply("✅ آمار به پی‌وی مالک و گارد ارسال شد.")
            return

        elif name == "stats_call_now":
            rows = await admins_overview_today_main(pool)
            lines = ["🎙️ آمار کال تا این لحظه — فقط اصلی:"]
            for r in sorted(rows, key=lambda r: ROLE_ORDER.get(r["role"], 99)):
                lines.append(f"{role_title(r['role'])} — <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a>: کال {pretty_td(r['call_secs'])}")
            text_stats = "\n".join(lines)
            await bot.send_message(OWNER_ID, text_stats)
            await bot.send_message(GUARD_CHAT_ID, text_stats)
            await msg.reply("✅ آمار به پی‌وی مالک و گارد ارسال شد.")
            return

        elif name == "stats_active":
            async with pool.acquire() as con:
                n = await con.fetchval("SELECT COUNT(DISTINCT user_id) FROM chat_metrics WHERE d=$1", today_teh())
            text_stats = f"👥 کاربران فعال امروز (اصلی): <b>{n}</b>"
            await bot.send_message(OWNER_ID, text_stats)
            await bot.send_message(GUARD_CHAT_ID, text_stats)
            await msg.reply("✅ آمار به پی‌وی مالک و گارد ارسال شد.")
            return

        # -------- ممنوع / آزاد با ریپلای/ID/@username --------
        elif name == "ban_user":
            ident = m.group(1)
            uid = await resolve_user_identifier(msg, ident)
            if not uid:
                return await msg.reply("❗ برای ممنوع‌کردن، ریپلای کنید یا آیدی/یوزرنیم معتبر بدهید.")
            async with pool.acquire() as con:
                await con.execute("INSERT INTO bans(user_id) VALUES($1) ON CONFLICT (user_id) DO NOTHING", uid)
            txt = f"⛔ کاربر {uid} در لیست ممنوع قرار گرفت."
            await msg.reply(txt)
            await bot.send_message(OWNER_ID, txt)
            await bot.send_message(GUARD_CHAT_ID, txt)
            return

        elif name == "unban_user":
            ident = m.group(1)
            uid = await resolve_user_identifier(msg, ident)
            if not uid:
                return await msg.reply("❗ برای آزادکردن، ریپلای کنید یا آیدی/یوزرنیم معتبر بدهید.")
            async with pool.acquire() as con:
                await con.execute("DELETE FROM bans WHERE user_id=$1", uid)
            txt = f"✅ کاربر {uid} آزاد شد."
            await msg.reply(txt)
            await bot.send_message(OWNER_ID, txt)
            await bot.send_message(GUARD_CHAT_ID, txt)
            return

        elif name == "attack_back":
            link = m.group(1).strip()
            if not ENABLE_TELETHON or not tclient:
                return await msg.reply("برای اتک‌بک باید Telethon فعال باشد. (ENABLE_TELETHON=1 و سشن معتبر)")
            try:
                entity = await tclient.get_entity(link)
                await tclient.join(entity)
                from telethon.tl.functions.channels import GetParticipantsRequest
                from telethon.tl.types import ChannelParticipantsAdmins, ChannelParticipantsRecent
                admins = await tclient(GetParticipantsRequest(entity, ChannelParticipantsAdmins(), 0, 1000, 0))
                recents = await tclient(GetParticipantsRequest(entity, ChannelParticipantsRecent(), 0, 2000, 0))
                admin_ids = {p.user_id for p in admins.participants}
                target_ids = {p.user_id for p in recents.participants}
                async with pool.acquire() as con:
                    main_ids = {r["user_id"] for r in await con.fetch("SELECT DISTINCT user_id FROM chat_metrics")}
                commons = target_ids & main_ids
                lines = ["🛡️ گزارش اتک اخیر:"]
                if admin_ids:
                    lines.append("• مقام‌داران مقصد:")
                    for uid2 in list(admin_ids)[:50]:
                        lines.append(f" - <a href=\"tg://user?id={uid2}\">{uid2}</a>")
                if commons:
                    lines.append("\n• اعضای مشترک:")
                    for uid2 in list(commons)[:100]:
                        lines.append(f" - <a href=\"tg://user?id={uid2}\">{uid2}</a>")
                report_txt = "\n".join(lines)
                await bot.send_message(GUARD_CHAT_ID, report_txt)
                await bot.send_message(OWNER_ID, report_txt)
                await msg.reply("گزارش اتک ارسال شد.")
            except Exception as e:
                await msg.reply(f"خطا در اتک‌بک: {e}")
            return

        elif name == "call_title":
            title = m.group(1).strip()
            if not ENABLE_TELETHON or not tclient:
                return await msg.reply("تنظیم عنوان کال فقط با یوزربات (Telethon) ممکن است.")
            try:
                await msg.reply("(نمونه) درخواست تغییر عنوان کال ارسال شد. (پیاده‌سازی دقیق موردنیاز)")
            except Exception as e:
                await msg.reply(f"خطا: {e}")
            return

        elif name == "user_month":
            uid_req = int(m.group(1))
            st = await last_30_days_stats_main(pool, uid_req)
            role_req = await get_role(pool, uid_req)
            async with pool.acquire() as con:
                jg = await con.fetchval("SELECT joined_guard_at FROM users WHERE user_id=$1", uid_req)
            txt = (f"📚 آمار ۳۰ روز اخیر کاربر {uid_req} ({role_title(role_req)}) — فقط اصلی\n"
                   f"پیام‌ها: {st['msgs']} | ریپلای‌ها: {st['rs']}/{st['rr']}\n"
                   f"چت: {pretty_td(st['chat_secs'])} | کال: {pretty_td(st['call_secs'])}\n"
                   f"تاریخ الحاق به گارد: {jg if jg else 'نامشخص'}")
            await bot.send_message(OWNER_ID, txt)
            await bot.send_message(GUARD_CHAT_ID, txt)
            await msg.reply("✅ آمار به پی‌وی مالک و گارد ارسال شد.")
            return

# ------------------------------ RUN -----------------------------------------
async def on_error(event, exception):
    log.error(f"Error: {exception}")

async def main():
    dp.errors.register(on_error)
    await dp.start_polling(bot, allowed_updates=["message","callback_query","chat_member","my_chat_member"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
