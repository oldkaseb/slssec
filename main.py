# -*- coding: utf-8 -*-
# bot.py — Souls / Souls Guard (single-file)
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
from aiogram.client.default import DefaultBotProperties  # aiogram 3.7+
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ---- Optional Telethon (userbot) for call-tracking + "attack back" ----------
ENABLE_TELETHON = os.getenv("ENABLE_TELETHON", "0") == "1"
if ENABLE_TELETHON:
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession
    TELETHON_SESSION = os.getenv("TELETHON_SESSION", "")
    API_ID = int(os.getenv("API_ID", "0") or "0")
    API_HASH = os.getenv("API_HASH", "")

# --------------------------- ENV & GLOBALS -----------------------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
OWNER_ID = int(os.environ["OWNER_ID"])
MAIN_CHAT_ID = int(os.environ["MAIN_CHAT_ID"])
GUARD_CHAT_ID = int(os.environ["GUARD_CHAT_ID"])
TEHRAN = ZoneInfo(os.getenv("TZ", "Asia/Tehran"))

# Roles (string constants)
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

# مجوز نقش‌ها برای دکمه‌های ورود
ALLOWED_VOICE_ROLES = {
    "owner", "senior_all", "senior_call", "admin_call",
    "senior_chat", "admin_chat",  # چتی‌ها هم اجازه دارند
}
ALLOWED_CHAT_ROLES = {
    "owner", "senior_all", "senior_chat", "admin_chat"
}

# In-memory ephemeral states
PENDING_REPORT = {}            # {user_id: {"type": "..."}}
PENDING_CONTACT_OWNER = set()
PENDING_CONTACT_GUARD = set()

# Manual voice (call) heartbeat fallback
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
        "senior_call": "🛡️ ارشد ویس",
        "admin_channel": "📢 ادمین کانال",
        "admin_chat": "💬 ادمین چت",
        "admin_call": "🎙️ ادمین ویس",
        "member": "👤 عضو"
    }.get(role, role)

# ----------------------------- Database --------------------------------------
SCHEMA_SQL = """
-- DDL ONLY (no parameterized statements here)
CREATE TABLE IF NOT EXISTS users(
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
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
    start_date DATE NOT NULL, -- Tehran day at start
    source TEXT
);

-- prevent multiple open sessions
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
    score INT, -- +1 راضی ، -1 ناراضی
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

-- performance indexes
CREATE INDEX IF NOT EXISTS idx_chat_metrics_user_d ON chat_metrics(user_id, d);
CREATE INDEX IF NOT EXISTS idx_sessions_date_kind_user ON sessions(start_date, kind, user_id);
CREATE INDEX IF NOT EXISTS idx_feedback_target_d ON feedback(target_user_id, d);
CREATE INDEX IF NOT EXISTS idx_candidates_daily_user_d ON candidates_daily(user_id, d);
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

async def admin_today_stats(pool, user_id: int):
    d = today_teh()
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
            FROM sessions WHERE user_id=$1 AND kind='chat' AND start_date=$2
        ),
        call_secs AS (
            SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) AS secs
            FROM sessions WHERE user_id=$1 AND kind='call' AND start_date=$2
        )
        SELECT cm.msgs, cm.r_sent, cm.r_recv, chat_secs.secs as chat_secs, call_secs.secs as call_secs
        FROM cm, chat_secs, call_secs
        """, user_id, d)
        return row

async def admins_overview_today(pool):
    d = today_teh()
    async with pool.acquire() as con:
        rows = await con.fetch("""
        WITH u AS (
            SELECT user_id, role, rank, username, first_name, last_name
            FROM users WHERE role <> 'member'
        ),
        chat_secs AS (
            SELECT user_id, COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) as chat_secs
            FROM sessions WHERE kind='chat' AND start_date=$1 GROUP BY user_id
        ),
        call_secs AS (
            SELECT user_id, COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) as call_secs
            FROM sessions WHERE kind='call' AND start_date=$1 GROUP BY user_id
        ),
        cm AS (
            SELECT user_id, COALESCE(SUM(msgs),0) as msgs
            FROM chat_metrics WHERE d=$1 GROUP BY user_id
        )
        SELECT u.*, COALESCE(cm.msgs,0) as msgs,
               COALESCE(chat_secs.chat_secs,0) as chat_secs,
               COALESCE(call_secs.call_secs,0) as call_secs
        FROM u
        LEFT JOIN cm ON cm.user_id=u.user_id
        LEFT JOIN chat_secs ON chat_secs.user_id=u.user_id
        LEFT JOIN call_secs ON call_secs.user_id=u.user_id
        """, d)
        return rows

async def top_candidates(pool, limit=10, days=7):
    start_d = today_teh() - timedelta(days=days)
    async with pool.acquire() as con:
        rows = await con.fetch("""
        WITH sums AS (
            SELECT user_id,
                   SUM(chat_msgs) AS chat_msgs,
                   SUM(call_seconds) AS call_seconds,
                   SUM(presence_seconds) AS presence_seconds
            FROM candidates_daily
            WHERE d >= $1
            GROUP BY user_id
        )
        SELECT s.*, u.username, u.first_name, u.last_name, u.role
        FROM sums s
        LEFT JOIN users u ON u.user_id=s.user_id
        WHERE COALESCE(u.role,'member')='member'
        ORDER BY chat_msgs DESC, call_seconds DESC
        LIMIT $2
        """, start_d, limit)
        return rows

async def add_feedback(pool, target_user: int, giver: int, score: int):
    async with pool.acquire() as con:
        await con.execute("""
        INSERT INTO feedback(target_user_id, giver_user_id, d, score)
        VALUES($1,$2,$3,$4)
        """, target_user, giver, today_teh(), score)

# ----------------------------- Keyboards -------------------------------------

def kb_checkin(kind: str, user_id: int):
    b = InlineKeyboardBuilder()
    if kind == "chat":
        b.button(text="✅ ثبت ورود چت", callback_data=f"ci:chat:{user_id}")
    else:
        b.button(text="✅ ثبت ورود ویس", callback_data=f"ci:call:{user_id}")
    b.button(text="❌ ثبت خروج", callback_data=f"co:{kind}:{user_id}")
    b.adjust(1,1)
    return b.as_markup()

def kb_first_msg_dual_checkin(user_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="✅ ثبت ورود چت",  callback_data=f"ci:chat:{user_id}")
    b.button(text="✅ ثبت ورود ویس", callback_data=f"ci:call:{user_id}")
    b.adjust(2)
    return b.as_markup()

def kb_feedback(target_user_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="👍 راضی", callback_data=f"fb:{target_user_id}:1")
    b.button(text="👎 ناراضی", callback_data=f"fb:{target_user_id}:-1")
    b.adjust(2)
    return b.as_markup()

def kb_admin_panel(role: str, is_owner: bool=False):
    """
    پنل مرتب، وابسته به نقش.
    """
    b = InlineKeyboardBuilder()

    # --- عمومی برای همه ---
    b.button(text="📊 آمار من", callback_data="pv:me")
    b.button(text="📈 آمار کلی من", callback_data="pv:me_all")
    b.button(text="✉️ ارتباط با مالک", callback_data="pv:contact_owner")
    b.button(text="📣 پیام به گارد", callback_data="pv:contact_guard")
    b.button(text="🚨 گزارش کاربر", callback_data="pv:report_user")

    # --- ابزار چت ---
    if role in {"admin_chat","senior_chat","senior_all"} or is_owner:
        b.button(text="🧑‍💻 لیست ادمین‌های چت", callback_data="pv:list_admins_chat")
        b.button(text="📝 پیام به گروه اصلی", callback_data="pv:send_to_main")
        b.button(text="📮 گزارش به مالک (چت)", callback_data="pv:send_report_owner")
        b.button(text="🚨 گزارش ادمین چت", callback_data="pv:report_admin_chat")

    # --- ابزار ویس (call) ---
    if role in {"admin_call","senior_call","senior_all"} or is_owner:
        b.button(text="🎙️ لیست ادمین‌های ویس", callback_data="pv:list_admins_voice")
        b.button(text="📝 پیام به گروه (ویس)", callback_data="pv:send_to_main_voice")
        b.button(text="📮 گزارش به مالک (ویس)", callback_data="pv:send_report_owner_voice")
        b.button(text="🚨 گزارش ادمین ویس", callback_data="pv:report_admin_voice")

    b.adjust(2)
    return b.as_markup()

# ----------------------------- Help / Guide ----------------------------------

def help_text_for_role(role: str, is_owner: bool=False) -> str:
    base = [
        "<b>راهنمای سریع پنل</b>",
        "• در گروه اصلی: اولین پیام امروز → دکمه‌های «ثبت ورود چت» و «ثبت ورود ویس».",
        "• خروج خودکار چت: ۱۰ دقیقه بی‌فعالی.",
        "• ویس: ثبت دستی (دکمه/متن).",
        "• /cancel برای لغو فرآیندهای درحال انجام.",
        "",
        "<b>دکمه‌های پیوی</b>",
        "📊 آمار من — 📈 آمار کلی من",
        "✉️ ارتباط با مالک — 📣 پیام به گارد",
        "🚨 گزارش کاربر",
    ]
    if role in {"senior_chat","senior_all"} or is_owner:
        base += [
            "",
            "<b>ابزار ارشد چت</b>",
            "🧑‍💻 لیست ادمین‌های چت",
            "📝 پیام به گروه اصلی",
            "📮 گزارش به مالک (چت)",
            "🚨 گزارش ادمین چت",
        ]
    if role in {"senior_call","senior_all"} or is_owner:
        base += [
            "",
            "<b>ابزار ارشد ویس</b>",
            "🎙️ لیست ادمین‌های ویس",
            "📝 پیام به گروه (ویس)",
            "📮 گزارش به مالک (ویس)",
            "🚨 گزارش ادمین ویس",
        ]
    base += [
        "",
        "<b>میان‌برهای گروه</b>",
        "گروه اصلی (چت): «ثبت ورود» — «ثبت خروج»",
        "گروه گارد (ویس): «ثبت ورود ویس» — «ثبت خروج ویس»",
    ]
    if is_owner or role == "owner":
        base += ["", "<i>✋ برای راهنمای کامل مالک «راهنما» را ارسال کنید.</i>"]
    return "\n".join(base)

def owner_help_text() -> str:
    return "\n".join([
        "<b>👑 راهنمای کامل مالک</b>",
        "",
        "<b>ترفیع/عزل نقش</b>",
        "• <code>ترفیع چت @username|id</code> — ادمین چت",
        "• <code>ترفیع ویس @username|id</code> یا <code>ترفیع کال ...</code> — ادمین ویس",
        "• <code>ترفیع ارشدچت @username|id</code> — ارشد چت",
        "• <code>ترفیع ارشدویس @username|id</code> یا <code>ترفیع ارشدکال ...</code> — ارشد ویس",
        "• <code>ترفیع ارشدکل @username|id</code> — ارشد کل",
        "• <code>عزل چت|ویس|کال|ارشدچت|ارشدویس|ارشدکال|ارشدکل @username|id</code>",
        "",
        "<b>آمار</b>",
        "• <code>آمار چت الان</code> — زمان چت + تعداد پیام‌ها",
        "• <code>آمار ویس الان</code> یا <code>آمار کال الان</code> — زمان ویس",
        "• <code>آمار</code> — تعداد کاربران فعال امروز",
        "• <code>آمار کلی کاربر id</code> — گزارش ۳۰ روز اخیر کاربر + دکمه رأی",
        "",
        "<b>بن/آنبن</b>",
        "• <code>ممنوع id</code> — افزودن به لیست ممنوع",
        "• <code>آزاد id</code> — حذف از لیست ممنوع",
        "",
        "<b>ویس (Telethon)</b>",
        "• <code>اتک بک &lt;لینک گروه&gt;</code> — گزارش ادمین‌ها و مشترک‌ها",
        "• <code>تایتل ویس &lt;متن&gt;</code> یا <code>تایتل کال &lt;متن&gt;</code> — (نمونه/قابل توسعه)",
        "",
        "<b>نکات</b>",
        "• پیام اول امروز در گروه اصلی → دکمه‌های ورود چت/ویس.",
        "• «ثبت ورود/خروج» چت در گروه اصلی؛ «ثبت ورود/خروج ویس» در گروه گارد.",
        "• خروج خودکار چت/ویس پس از ۱۰ دقیقه بی‌فعالی.",
    ])

# ----------------------------- Bot Init --------------------------------------
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TEHRAN)
pool: asyncpg.Pool = None
tclient: "TelegramClient|None" = None

# ----------------------------- Help Handlers ---------------------------------
@dp.message(Command(commands=["help"]) , F.chat.type == ChatType.PRIVATE)
async def help_pv(msg: Message):
    role = await get_role(pool, msg.from_user.id)
    is_owner = (msg.from_user.id == OWNER_ID)
    if is_owner:
        return await msg.answer(owner_help_text())
    await msg.answer(help_text_for_role(role, is_owner))

@dp.message(F.text.regexp(r"^(?:راهنما|help|/?help)$"))
async def help_anywhere(msg: Message):
    if msg.from_user.id == OWNER_ID:
        return await msg.reply(owner_help_text())
    role = await get_role(pool, msg.from_user.id)
    await msg.reply(help_text_for_role(role, is_owner=False))

@dp.message(((F.chat.type == ChatType.GROUP) | (F.chat.type == ChatType.SUPERGROUP)), Command(commands=["help"]))
async def help_group(msg: Message):
    if msg.from_user.id == OWNER_ID:
        return await msg.reply(owner_help_text())
    await msg.reply("راهنما به پیوی ارسال شد. /start را در پیوی بزنید.")
    try:
        await bot.send_message(msg.from_user.id, help_text_for_role(
            await get_role(pool, msg.from_user.id),
            msg.from_user.id == OWNER_ID
        ))
    except Exception:
        pass

# ---------------------------- Startup ----------------------------------------
async def on_startup():
    global pool, tclient
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as con:
        for stmt in [s.strip() for s in SCHEMA_SQL.split(";") if s.strip()]:
            await con.execute(stmt + ";")

        await con.execute(
            """
            INSERT INTO groups (group_type, chat_id, title)
            VALUES ('main', $1, 'souls')
            ON CONFLICT (group_type) DO UPDATE
                SET chat_id = EXCLUDED.chat_id,
                    title = EXCLUDED.title
            """, MAIN_CHAT_ID,
        )
        await con.execute(
            """
            INSERT INTO groups (group_type, chat_id, title)
            VALUES ('guard', $1, 'souls guard')
            ON CONFLICT (group_type) DO UPDATE
                SET chat_id = EXCLUDED.chat_id,
                    title = EXCLUDED.title
            """, GUARD_CHAT_ID,
        )
    log.info("DB ready.")

    if ENABLE_TELETHON and 'API_ID' in globals() and 'API_HASH' in globals() and 'TELETHON_SESSION' in globals() and API_ID and API_HASH and TELETHON_SESSION:
        tclient = TelegramClient(StringSession(TELETHON_SESSION), API_ID, API_HASH)
        await tclient.start()
        log.info("Telethon userbot started.")

        @tclient.on(events.Raw)
        async def telethon_raw_handler(event):
            if event.__class__.__name__ == "UpdateGroupCallParticipants":
                try:
                    pass
                except Exception as e:
                    log.warning(f"Telethon handler error: {e}")
    else:
        log.info("Telethon disabled or not configured.")

    scheduler.add_job(job_autoclose_inactive_chat, CronTrigger.from_crontab("*/1 * * * *"))
    scheduler.add_job(job_autoclose_inactive_call_fallback, CronTrigger.from_crontab("*/1 * * * *"))
    scheduler.add_job(job_daily_rollover, CronTrigger(hour=0, minute=0))
    scheduler.start()
    log.info("Scheduler started.")

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
            txt = f"⏹️ خروج خودکار ویس برای {mention} پس از ۱۰ دقیقه ثبت شد."
            await bot.send_message(GUARD_CHAT_ID, txt)
            await bot.send_message(OWNER_ID, txt)
        except Exception:
            pass
        CALL_HEARTBEATS.pop(uid, None)

async def job_daily_rollover():
    try:
        rows = await admins_overview_today(pool)
        if not rows:
            return
        lines = ["📊 <b>آمار امروز ادمین‌ها</b>\n(از ۰۰:۰۰ تا اکنون به وقت تهران)\n"]
        rows_sorted = sorted(rows, key=lambda r: (ROLE_ORDER.get(r["role"], 999), -r["msgs"], -r["call_secs"]))
        for r in rows_sorted:
            name = r["first_name"] or ""
            un = f"@{r['username']}" if r["username"] else ""
            rt = role_title(r["role"])
            lines.append(
                f"{rt} — <a href=\"tg://user?id={r['user_id']}\">{name or r['user_id']}</a> {un}\n"
                f"• پیام‌ها: <b>{r['msgs']}</b> | چت: <b>{pretty_td(r['chat_secs'])}</b> | ویس: <b>{pretty_td(r['call_secs'])}</b>"
            )
        text = "\n".join(lines)
        await bot.send_message(OWNER_ID, text)
        await bot.send_message(GUARD_CHAT_ID, text)

        cands = await top_candidates(pool, 10, 7)
        if cands:
            clines = ["🏆 <b>۱۰ عضو برتر (۷ روز اخیر)</b>"]
            rank = 1
            for c in cands:
                nm = c["first_name"] or ""
                un = f"@{c['username']}" if c["username"] else ""
                clines.append(
                    f"{rank}. <a href=\"tg://user?id={c['user_id']}\">{nm or c['user_id']}</a> {un} — "
                    f"چت: {c['chat_msgs']} | ویس: {pretty_td(c['call_seconds'])} | حضور: {pretty_td(c['presence_seconds'])}"
                )
                rank += 1
            await bot.send_message(OWNER_ID, "\n".join(clines))

    except Exception as e:
        log.error(f"job_daily_rollover: {e}")

# ------------------------------ Handlers -------------------------------------

# /start در پیوی
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
            reply_markup=kb_admin_panel(
                role,
                is_owner=(msg.from_user.id==OWNER_ID),
            )
        )
    else:
        await msg.answer(
            "این ربات مخصوص ادمین‌های گارد سولز است.\n"
            "برای ارتباط با مالک از ربات @soulsownerbot استفاده کنید.",
            reply_markup=ReplyKeyboardRemove()
        )

# پیام‌های گروه اصلی: شمارش و پیشنهاد ورود
@dp.message(F.chat.id == MAIN_CHAT_ID, F.from_user)
async def main_group_messages(msg: Message):
    u = msg.from_user
    await ensure_user(pool, u)

    # بن‌ها
    async with pool.acquire() as con:
        banned = await con.fetchval("SELECT 1 FROM bans WHERE user_id=$1", u.id)
    if banned:
        try:
            await bot.delete_message(MAIN_CHAT_ID, msg.message_id)
        except Exception:
            pass
        return

    role = await get_role(pool, u.id)

    # شمارش پیام‌ها، ریپلای‌ها — برای همه (ادمین‌ها + اعضا)
    await inc_chat_metrics(pool, u.id, msg)

    # ===== میان‌برهای متنی گروه برای ورود/خروج چت =====
    if msg.text:
        t = msg.text.strip().lower()
        if t in {"ثبت ورود","ورود"} and role in ALLOWED_CHAT_ROLES:
            if await count_open(pool, u.id, "chat") == 0:
                await open_session(pool, u.id, "chat", source="text_group")
                await msg.reply("✅ ورود چت ثبت شد.")
                await bot.send_message(GUARD_CHAT_ID, f"✅ <a href=\"tg://user?id={u.id}\">{u.first_name}</a> ورود چت زد.")
                await bot.send_message(OWNER_ID, f"✅ <a href=\"tg://user?id={u.id}\">{u.first_name}</a> ورود چت زد.")
            else:
                await msg.reply("سشن چت باز داری.")
            return
        if t in {"ثبت خروج","خروج"} and role in ALLOWED_CHAT_ROLES:
            await close_session(pool, u.id, "chat")
            await msg.reply("⏹️ خروج چت ثبت شد.")
            await bot.send_message(GUARD_CHAT_ID, f"⏹️ <a href=\"tg://user?id={u.id}\">{u.first_name}</a> خروج چت زد.")
            await bot.send_message(OWNER_ID, f"⏹️ <a href=\"tg://user?id={u.id}\">{u.first_name}</a> خروج چت زد.")
            return

    # ===== پیشنهاد خودکار با «اولین پیام امروز» =====
    try:
        async with pool.acquire() as con:
            todays_msgs = await con.fetchval(
                "SELECT msgs FROM chat_metrics WHERE user_id=$1 AND d=$2",
                u.id, today_teh()
            ) or 0
        is_first_msg_today = (todays_msgs == 1)
    except Exception:
        is_first_msg_today = False

    # فقط روی اولین پیام امروز و برای نقش‌های مجاز؛ دو دکمه ورود (بدون خروج)
    if is_first_msg_today and (role in ALLOWED_CHAT_ROLES or role in ALLOWED_VOICE_ROLES):
        return await msg.reply(
            f"اولین پیام امروز ثبت شد. {u.first_name} عزیز، یکی از گزینه‌ها را بزن:",
            reply_markup=kb_first_msg_dual_checkin(u.id)
        )

    # اگر سشن چت باز دارد، لمس فعالیت برای جلوگیری از خروج خودکار
    if role in ALLOWED_CHAT_ROLES and await count_open(pool, u.id, "chat") > 0:
        await touch_activity(pool, u.id, "chat")

# ویس: میان‌بر متنی در گروه گارد
@dp.message(F.chat.id == GUARD_CHAT_ID, F.text.regexp(r"^(ثبت ورود ویس|ثبت خروج ویس)$"))
async def guard_group_voice_text(msg: Message):
    u = msg.from_user
    await ensure_user(pool, u)
    role = await get_role(pool, u.id)
    if role not in ALLOWED_VOICE_ROLES:
        return
    if msg.text == "ثبت ورود ویس":
        await open_session(pool, u.id, "call", source="manual")
        CALL_HEARTBEATS[u.id] = now_teh()
        await msg.reply("✅ ورود ویس ثبت شد. (در صورت بی‌فعالی ۱۰ دقیقه‌ای خروج خودکار می‌خوری)")
        await bot.send_message(OWNER_ID, f"🎙️ ورود ویس: <a href=\"tg://user?id={u.id}\">{u.first_name}</a>")
    else:
        await close_session(pool, u.id, "call")
        CALL_HEARTBEATS.pop(u.id, None)
        await msg.reply("⏹️ خروج ویس ثبت شد.")
        await bot.send_message(OWNER_ID, f"🎙️ خروج ویس: <a href=\"tg://user?id={u.id}\">{u.first_name}</a>")

# ویس: دکمه شیشه‌ای از گروه اصلی (اختیاری با پیام "ویس")
@dp.message(F.chat.id == MAIN_CHAT_ID, F.text.regexp(r"^ویس$"))
async def main_group_voice_help(msg: Message):
    role = await get_role(pool, msg.from_user.id)
    if role not in ALLOWED_VOICE_ROLES:
        return await msg.reply("این بخش مخصوص ادمین‌های چت/ویس، ارشدها و مالک است.")
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ثبت ورود ویس", callback_data=f"ci:call:{msg.from_user.id}")
    kb.button(text="❌ ثبت خروج ویس", callback_data=f"co:call:{msg.from_user.id}")
    kb.adjust(2)
    await msg.reply("برای ویس یکی از گزینه‌ها را بزن:", reply_markup=kb.as_markup())

# کال‌بک‌های ورود/خروج (با کنترل نقش برای ویس)
@dp.callback_query(F.data.regexp(r"^(ci|co):(chat|call):(\d+)$"))
async def cb_checkin_out(cb: CallbackQuery):
    action, kind, uid = cb.data.split(":")
    uid = int(uid)
    if cb.from_user.id != uid and cb.from_user.id != OWNER_ID:
        return await cb.answer("این دکمه مخصوص همان کاربر/مالک است.", show_alert=True)
    await ensure_user(pool, cb.from_user)

    if kind == "call":
        role = await get_role(pool, cb.from_user.id)
        if role not in ALLOWED_VOICE_ROLES and cb.from_user.id != OWNER_ID:
            return await cb.answer("اجازه دسترسی ندارید.", show_alert=True)

    if action == "ci":
        if await count_open(pool, uid, kind) > 0:
            await cb.answer("सشن باز داری.", show_alert=True); return
        await open_session(pool, uid, kind, source="inline")
        if kind == "call":
            CALL_HEARTBEATS[uid] = now_teh()
        await cb.message.edit_text(f"✅ ثبت ورود {('چت' if kind=='chat' else 'ویس')} انجام شد.")
        mention = f"<a href=\"tg://user?id={uid}\">{cb.from_user.first_name}</a>"
        await bot.send_message(GUARD_CHAT_ID, f"✅ {mention} ورود {('چت' if kind=='chat' else 'ویس')} زد.")
        await bot.send_message(OWNER_ID, f"✅ {mention} ورود {('چت' if kind=='chat' else 'ویس')} زد.")
    else:
        await close_session(pool, uid, kind)
        if kind == "call":
            CALL_HEARTBEATS.pop(uid, None)
        await cb.message.edit_text(f"⏹️ خروج {('چت' if kind=='chat' else 'ویس')} ثبت شد.")
        mention = f"<a href=\"tg://user?id={uid}\">{cb.from_user.first_name}</a>"
        await bot.send_message(GUARD_CHAT_ID, f"⏹️ {mention} خروج {('چت' if kind=='chat' else 'ویس')} زد.")
        await bot.send_message(OWNER_ID, f"⏹️ {mention} خروج {('چت' if kind=='chat' else 'ویس')} زد.")
    await cb.answer()

# پنل پیوی دکمه‌ها (با کنترل دسترسی صریح)
@dp.callback_query(F.data.startswith("pv:"))
async def pv_buttons(cb: CallbackQuery):
    await ensure_user(pool, cb.from_user)
    role = await get_role(pool, cb.from_user.id)
    is_owner = (cb.from_user.id == OWNER_ID)

    if cb.data == "pv:me":
        st = await admin_today_stats(pool, cb.from_user.id)
        if st:
            txt = (f"📊 <b>آمار امروز شما</b>\n"
                   f"پیام‌ها: <b>{st['msgs']}</b>\n"
                   f"ریپلای‌ها (ارسال/دریافت): <b>{st['r_sent']}/{st['r_recv']}</b>\n"
                   f"زمان چت: <b>{pretty_td(st['chat_secs'])}</b>\n"
                   f"زمان ویس: <b>{pretty_td(st['call_secs'])}</b>\n")
            await cb.message.edit_text(txt, reply_markup=kb_admin_panel(role, is_owner))
        return await cb.answer()

    if cb.data == "pv:me_all":
        async with pool.acquire() as con:
            st = await con.fetchrow("""
                WITH cm AS (
                    SELECT COALESCE(SUM(msgs),0) msgs, COALESCE(SUM(replies_sent),0) rs,
                           COALESCE(SUM(replies_received),0) rr
                    FROM chat_metrics WHERE user_id=$1 AND d >= $2
                ),
                sess AS (
                    SELECT kind, COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) secs
                    FROM sessions WHERE user_id=$1 AND start_date >= $2 GROUP BY kind
                )
                SELECT cm.msgs, cm.rs, cm.rr,
                    COALESCE((SELECT secs FROM sess WHERE kind='chat'),0) chat_secs,
                    COALESCE((SELECT secs FROM sess WHERE kind='call'),0) call_secs
            """, cb.from_user.id, today_teh()-timedelta(days=30))
        txt = (f"📈 <b>۳۰ روز اخیر شما</b>\n"
               f"پیام‌ها: <b>{st['msgs']}</b>\n"
               f"ریپلای‌ها (ارسال/دریافت): <b>{st['rs']}/{st['rr']}</b>\n"
               f"چت: <b>{pretty_td(st['chat_secs'])}</b> | ویس: <b>{pretty_td(st['call_secs'])}</b>")
        await cb.message.edit_text(txt, reply_markup=kb_admin_panel(role, is_owner))
        return await cb.answer()

    if cb.data == "pv:contact_owner":
        PENDING_CONTACT_OWNER.add(cb.from_user.id)
        await cb.message.edit_text("پیام‌تان به مالک را ارسال کنید (لغو: /cancel)")
        return await cb.answer()

    if cb.data == "pv:contact_guard":
        PENDING_CONTACT_GUARD.add(cb.from_user.id)
        await cb.message.edit_text("پیام شما به گروه گارد ارسال می‌شود: الان متن را بفرستید. (لغو: /cancel)")
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
        lines = ["🎙️ ادمین‌های ویس:"]
        for r in rows:
            lines.append(f"• {role_title(r['role'])}: <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a> @{r['username'] or ''}")
        await cb.message.edit_text("\n".join(lines))
        return await cb.answer()

    if cb.data in {"pv:send_to_main","pv:send_report_owner","pv:report_admin_chat"}:
        if not (role in {"admin_chat","senior_chat","senior_all"} or is_owner):
            return await cb.answer("اجازه دسترسی ندارید.", show_alert=True)
        await cb.message.edit_text("متن خود را ارسال کنید. (لغو: /cancel)")
        PENDING_REPORT[cb.from_user.id] = {"type": cb.data}
        return await cb.answer()

    if cb.data in {"pv:send_to_main_voice","pv:send_report_owner_voice","pv:report_admin_voice"}:
        if not (role in {"admin_call","senior_call","senior_all"} or is_owner):
            return await cb.answer("اجازه دسترسی ندارید.", show_alert=True)
        await cb.message.edit_text("متن خود را ارسال کنید. (لغو: /cancel)")
        PENDING_REPORT[cb.from_user.id] = {"type": cb.data}
        return await cb.answer()

    return await cb.answer()

# دریافت متن‌های پس از دکمه‌های پیوی + fallbackهای متنی
@dp.message(F.chat.type == ChatType.PRIVATE)
async def pv_text_flow(msg: Message):
    uid = msg.from_user.id
    role = await get_role(pool, uid)

    if msg.text == "/cancel":
        PENDING_CONTACT_GUARD.discard(uid)
        PENDING_CONTACT_OWNER.discard(uid)
        PENDING_REPORT.pop(uid, None)
        return await msg.reply(
            "لغو شد.",
            reply_markup=kb_admin_panel(role, is_owner=(uid==OWNER_ID))
        )

    t = (msg.text or "").strip().lower()

    # پنل
    if t in {"پنل","panel","menu","منو","/panel"}:
        return await msg.answer(
            "پنل شما:",
            reply_markup=kb_admin_panel(role, is_owner=(uid==OWNER_ID))
        )

    # آمار
    if t in {"آمار من","stats me","/me"}:
        st = await admin_today_stats(pool, uid)
        if st:
            txt = (f"📊 <b>آمار امروز شما</b>\n"
                   f"پیام‌ها: <b>{st['msgs']}</b>\n"
                   f"ریپلای‌ها (ارسال/دریافت): <b>{st['r_sent']}/{st['r_recv']}</b>\n"
                   f"زمان چت: <b>{pretty_td(st['chat_secs'])}</b>\n"
                   f"زمان ویس: <b>{pretty_td(st['call_secs'])}</b>\n")
            return await msg.answer(txt)

    if t in {"آمار کلی من","stats all","/me_all"}:
        async with pool.acquire() as con:
            st = await con.fetchrow(
                """
                WITH cm AS (
                    SELECT COALESCE(SUM(msgs),0) msgs, COALESCE(SUM(replies_sent),0) rs,
                           COALESCE(SUM(replies_received),0) rr
                    FROM chat_metrics WHERE user_id=$1 AND d >= $2
                ),
                sess AS (
                    SELECT kind, COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) secs
                    FROM sessions WHERE user_id=$1 AND start_date >= $2 GROUP BY kind
                )
                SELECT cm.msgs, cm.rs, cm.rr,
                    COALESCE((SELECT secs FROM sess WHERE kind='chat'),0) chat_secs,
                    COALESCE((SELECT secs FROM sess WHERE kind='call'),0) call_secs
                """,
                uid, today_teh()-timedelta(days=30)
            )
        txt = (f"📈 <b>۳۰ روز اخیر شما</b>\n"
               f"پیام‌ها: <b>{st['msgs']}</b>\n"
               f"ریپلای‌ها (ارسال/دریافت): <b>{st['rs']}/{st['rr']}</b>\n"
               f"چت: <b>{pretty_td(st['chat_secs'])}</b> | ویس: <b>{pretty_td(st['call_secs'])}</b>")
        return await msg.answer(txt)

    # ارتباط/گزارش
    if t in {"ارتباط با مالک","contact owner"}:
        PENDING_CONTACT_OWNER.add(uid)
        return await msg.answer("پیام‌تان به مالک را ارسال کنید (لغو: /cancel)")

    if t in {"پیام به گارد","contact guard"}:
        PENDING_CONTACT_GUARD.add(uid)
        return await msg.answer("پیام شما به گروه گارد ارسال می‌شود: الان متن را بفرستید. (لغو: /cancel)")

    if t in {"گزارش کاربر","report user"}:
        PENDING_REPORT[uid] = {"type": "member"}
        return await msg.answer("آیدی عددی یا یوزرنیم کاربر را بفرستید.")

    # لیست‌ها (وابسته به نقش)
    if (role in {"senior_chat","senior_all"} or uid==OWNER_ID or role=="admin_chat") and t in {"لیست ادمین‌های چت","admins chat"}:
        async with pool.acquire() as con:
            rows = await con.fetch("SELECT user_id, username, first_name, role FROM users WHERE role IN ('admin_chat','senior_chat','senior_all','owner') ORDER BY role")
        lines = ["🧑‍💻 ادمین‌های چت:"]
        for r in rows:
            lines.append(f"• {role_title(r['role'])}: <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a> @{r['username'] or ''}")
        return await msg.answer("\n".join(lines))

    if (role in {"senior_call","senior_all"} or uid==OWNER_ID or role=="admin_call") and t in {"لیست ادمین‌های ویس","admins voice"}:
        async with pool.acquire() as con:
            rows = await con.fetch("SELECT user_id, username, first_name, role FROM users WHERE role IN ('admin_call','senior_call','senior_all','owner') ORDER BY role")
        lines = ["🎙️ ادمین‌های ویس:"]
        for r in rows:
            lines.append(f"• {role_title(r['role'])}: <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a> @{r['username'] or ''}")
        return await msg.answer("\n".join(lines))

    # ارسال/گزارش (چت)
    if (role in {"admin_chat","senior_chat","senior_all"} or uid==OWNER_ID) and t in {"ارسال پیام به گروه","send to main"}:
        PENDING_REPORT[uid] = {"type": "pv:send_to_main"}
        return await msg.answer("متن خود را ارسال کنید. (لغو: /cancel)")

    if (role in {"admin_chat","senior_chat","senior_all"} or uid==OWNER_ID) and t in {"ارسال گزارش به مالک","send report owner"}:
        PENDING_REPORT[uid] = {"type": "pv:send_report_owner"}
        return await msg.answer("متن خود را ارسال کنید. (لغو: /cancel)")

    if (role in {"admin_chat","senior_chat","senior_all"} or uid==OWNER_ID) and t in {"گزارش ادمین چت به مالک","report admin chat"}:
        PENDING_REPORT[uid] = {"type": "pv:report_admin_chat"}
        return await msg.answer("نام ادمین/گزارش را ارسال کنید. (لغو: /cancel)")

    # ارسال/گزارش (ویس)
    if (role in {"admin_call","senior_call","senior_all"} or uid==OWNER_ID) and t in {"پیام به گروه (ویس)","send to main voice"}:
        PENDING_REPORT[uid] = {"type": "pv:send_to_main_voice"}
        return await msg.answer("متن خود را ارسال کنید. (لغو: /cancel)")

    if (role in {"admin_call","senior_call","senior_all"} or uid==OWNER_ID) and t in {"گزارش به مالک (ویس)","send report owner voice"}:
        PENDING_REPORT[uid] = {"type": "pv:send_report_owner_voice"}
        return await msg.answer("متن خود را ارسال کنید. (لغو: /cancel)")

    if (role in {"admin_call","senior_call","senior_all"} or uid==OWNER_ID) and t in {"گزارش ادمین ویس به مالک","report admin voice"}:
        PENDING_REPORT[uid] = {"type": "pv:report_admin_voice"}
        return await msg.answer("نام ادمین/گزارش را ارسال کنید. (لغو: /cancel)")

    # ====== pending flows ======
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
        ttype = ctx["type"]
        if ttype == "member":
            await bot.send_message(OWNER_ID, f"🚨 گزارش از <a href=\"tg://user?id={uid}\">{msg.from_user.first_name}</a>:\n{msg.text}")
            return await msg.reply("گزارش به مالک ارسال شد ✅")
        else:
            if ttype == "pv:send_to_main":
                if not (role in {"admin_chat","senior_chat","senior_all"} or uid==OWNER_ID):
                    return await msg.reply("اجازه دسترسی ندارید.")
                await bot.send_message(MAIN_CHAT_ID, f"📝 پیام از ارشد/ادمین (چت):\n{msg.text}")
            elif ttype == "pv:send_to_main_voice":
                if not (role in {"admin_call","senior_call","senior_all"} or uid==OWNER_ID):
                    return await msg.reply("اجازه دسترسی ندارید.")
                await bot.send_message(MAIN_CHAT_ID, f"📝 پیام از ارشد/ادمین (ویس):\n{msg.text}")
            elif ttype == "pv:send_report_owner":
                if not (role in {"admin_chat","senior_chat","senior_all"} or uid==OWNER_ID):
                    return await msg.reply("اجازه دسترسی ندارید.")
                await bot.send_message(OWNER_ID, f"📮 گزارش (چت):\n{msg.text}")
            elif ttype == "pv:send_report_owner_voice":
                if not (role in {"admin_call","senior_call","senior_all"} or uid==OWNER_ID):
                    return await msg.reply("اجازه دسترسی ندارید.")
                await bot.send_message(OWNER_ID, f"📮 گزارش (ویس):\n{msg.text}")
            elif ttype == "pv:report_admin_chat":
                if not (role in {"admin_chat","senior_chat","senior_all"} or uid==OWNER_ID):
                    return await msg.reply("اجازه دسترسی ندارید.")
                await bot.send_message(OWNER_ID, f"🚨 گزارش ادمین چت:\n{msg.text}")
            elif ttype == "pv:report_admin_voice":
                if not (role in {"admin_call","senior_call","senior_all"} or uid==OWNER_ID):
                    return await msg.reply("اجازه دسترسی ندارید.")
                await bot.send_message(OWNER_ID, f"🚨 گزارش ادمین ویس:\n{msg.text}")
            return await msg.reply("انجام شد ✅")

# دکمه رأی مالک
@dp.callback_query(F.data.regexp(r"^fb:(\d+):(-?1)$"))
async def feedback_cb(cb: CallbackQuery):
    target, score = cb.data.split(":")[1:]
    target = int(target); score = int(score)
    if cb.from_user.id != OWNER_ID:
        return await cb.answer("فقط مالک می‌تواند رأی دهد.", show_alert=True)
    await add_feedback(pool, target, OWNER_ID, score)
    await cb.answer("ثبت شد.", show_alert=False)
    await cb.message.edit_reply_markup(reply_markup=None)

# ----------------------- دستورهای متنی مالک (بدون /) -----------------------
OWNER_CMD_PATTERNS = [
    (r"^(ترفیع|عزل)\s+(چت|ویس|کال|ارشدچت|ارشدویس|ارشدکال|ارشدکل)\s+(@\w+|\d+)$", "promote_demote"),
    (r"^آمار\s*چت\s*الان$", "stats_chat_now"),
    (r"^آمار\s*(?:ویس|کال)\s*الان$", "stats_call_now"),
    (r"^آمار\s*$", "stats_active"),
    (r"^ممنوع\s+(\d+)$", "ban_user"),
    (r"^آزاد\s+(\d+)$", "unban_user"),
    (r"^اتک\s*بک\s+(.+)$", "attack_back"),
    (r"^تایتل\s*(?:ویس|کال)\s+(.+)$", "call_title"),
    (r"^آمار\s*کلی\s*کاربر\s+(\d+)$", "user_month")
]

ROLE_MAP = {
    "چت": "admin_chat",
    "ویس": "admin_call",
    "کال": "admin_call",
    "ارشدچت": "senior_chat",
    "ارشدویس": "senior_call",
    "ارشدکال": "senior_call",
    "ارشدکل": "senior_all"
}

@dp.message(F.from_user.id == OWNER_ID)
async def owner_text_commands(msg: Message):
    text = (msg.text or "").strip()
    if re.fullmatch(r"(?:راهنما|help|/?help)", text):
        return await msg.reply(owner_help_text())

    for pat, name in OWNER_CMD_PATTERNS:
        m = re.match(pat, text)
        if not m:
            continue
        if name == "promote_demote":
            act, kind, ident = m.groups()
            target_id = None
            if ident.startswith("@"):
                try:
                    u = await bot.get_chat(ident)
                    target_id = u.id
                except Exception:
                    return await msg.reply("یوزرنیم یافت نشد.")
            else:
                target_id = int(ident)
            role_key = ROLE_MAP[kind]
            if act == "ترفیع":
                await set_role(pool, target_id, role_key)
                await msg.reply(f"✅ {target_id} به {role_title(role_key)} ترفیع یافت.")
            else:
                await set_role(pool, target_id, "member")
                await msg.reply(f"✅ {target_id} عزل شد.")
            return
        elif name == "stats_chat_now":
            rows = await admins_overview_today(pool)
            lines = ["📊 آمار چت تا این لحظه:"]
            for r in sorted(rows, key=lambda r: ROLE_ORDER.get(r["role"], 99)):
                lines.append(f"{role_title(r['role'])} — <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a>: چت {pretty_td(r['chat_secs'])} | پیام {r['msgs']}")
            await msg.reply("\n".join(lines)); return
        elif name == "stats_call_now":
            rows = await admins_overview_today(pool)
            lines = ["🎙️ آمار ویس تا این لحظه:"]
            for r in sorted(rows, key=lambda r: ROLE_ORDER.get(r["role"], 99)):
                lines.append(f"{role_title(r['role'])} — <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a>: ویس {pretty_td(r['call_secs'])}")
            await msg.reply("\n".join(lines)); return
        elif name == "stats_active":
            async with pool.acquire() as con:
                n = await con.fetchval("SELECT COUNT(DISTINCT user_id) FROM chat_metrics WHERE d=$1", today_teh())
            await msg.reply(f"👥 کاربران فعال امروز: <b>{n}</b>"); return
        elif name == "ban_user":
            uid = int(m.group(1))
            async with pool.acquire() as con:
                await con.execute("INSERT INTO bans(user_id) VALUES($1) ON CONFLICT (user_id) DO NOTHING", uid)
            await msg.reply(f"⛔ کاربر {uid} در لیست ممنوع قرار گرفت."); return
        elif name == "unban_user":
            uid = int(m.group(1))
            async with pool.acquire() as con:
                await con.execute("DELETE FROM bans WHERE user_id=$1", uid)
            await msg.reply(f"✅ کاربر {uid} آزاد شد."); return
        elif name == "attack_back":
            link = m.group(1).strip()
            if not ENABLE_TELETHON or 'tclient' not in globals() or not tclient:
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
                await bot.send_message(GUARD_CHAT_ID, "\n".join(lines))
                await msg.reply("گزارش اتک ارسال شد.")
            except Exception as e:
                await msg.reply(f"خطا در اتک‌بک: {e}")
            return
        elif name == "call_title":
            title = m.group(1).strip()
            if not ENABLE_TELETHON or 'tclient' not in globals() or not tclient:
                return await msg.reply("تنظیم عنوان ویس فقط با یوزربات (Telethon) ممکن است.")
            try:
                await msg.reply("(نمونه) درخواست تغییر عنوان ویس ارسال شد. (پیاده‌سازی دقیق موردنیاز)")
            except Exception as e:
                await msg.reply(f"خطا: {e}")
            return
        elif name == "user_month":
            uid_req = int(m.group(1))
            async with pool.acquire() as con:
                st = await con.fetchrow("""
                    WITH cm AS (
                        SELECT COALESCE(SUM(msgs),0) msgs,
                               COALESCE(SUM(replies_sent),0) rs,
                               COALESCE(SUM(replies_received),0) rr
                        FROM chat_metrics WHERE user_id=$1 AND d >= $2
                    ),
                    sess AS (
                        SELECT kind, COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_at, now()) - start_at)))::INT,0) secs
                        FROM sessions WHERE user_id=$1 AND start_date >= $2 GROUP BY kind
                    )
                    SELECT cm.msgs, cm.rs, cm.rr,
                           COALESCE((SELECT secs FROM sess WHERE kind='chat'),0) chat_secs,
                           COALESCE((SELECT secs FROM sess WHERE kind='call'),0) call_secs
                """, uid_req, today_teh()-timedelta(days=30))
                role_req = await get_role(pool, uid_req)
                jg = await con.fetchval("SELECT joined_guard_at FROM users WHERE user_id=$1", uid_req)
            txt = (f"📚 آمار ۳۰ روز اخیر کاربر {uid_req} ({role_title(role_req)})\n"
                   f"پیام‌ها: {st['msgs']} | ریپلای‌ها: {st['rs']}/{st['rr']}\n"
                   f"چت: {pretty_td(st['chat_secs'])} | ویس: {pretty_td(st['call_secs'])}\n"
                   f"تاریخ الحاق به گارد: {jg if jg else 'نامشخص'}")
            await msg.reply(txt, reply_markup=kb_feedback(uid_req))
            return
    # fallthrough

# ------------------------------ Misc -----------------------------------------
async def on_error(event, exception):
    log.error(f"Error: {exception}")

# ------------------------------- RUN -----------------------------------------
async def main():
    dp.errors.register(on_error)
    await dp.start_polling(bot, allowed_updates=["message","callback_query","chat_member","my_chat_member"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
