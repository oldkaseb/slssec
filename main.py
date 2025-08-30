# -*- coding: utf-8 -*-
# bot.py — Souls / Souls Guard (single-file)
# Python 3.11+
import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode, ChatType
from aiogram.client.default import DefaultBotProperties  # ✅ NEW for aiogram 3.7+
from aiogram.filters import CommandStart
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove
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

# In-memory ephemeral states (ok for Railway restarts)
PENDING_REPORT = {}            # {user_id: {"type": "member"/"admin", "target_user_id": int}}
PENDING_CONTACT_OWNER = set()  # user_ids awaiting text to forward
PENDING_CONTACT_GUARD = set()
PENDING_OWNER_REPORT_TARGET = {} # owner asks for "آمار کاربر مشخص" etc.

# For manual call 'heartbeat' (fallback when Telethon disabled)
CALL_HEARTBEATS = {}  # {user_id: datetime of last heartbeat}

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

# ----------------------------- Database --------------------------------------
SCHEMA_SQL = """
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

INSERT INTO groups (group_type, chat_id, title)
VALUES ('main', $1, 'souls')
ON CONFLICT (group_type) DO UPDATE SET chat_id=EXCLUDED.chat_id;

INSERT INTO groups (group_type, chat_id, title)
VALUES ('guard', $2, 'souls guard')
ON CONFLICT (group_type) DO UPDATE SET chat_id=EXCLUDED.chat_id;

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

-- برای اعضای معمولی جهت پیشنهاد ادمینی
CREATE TABLE IF NOT EXISTS candidates_daily(
    user_id BIGINT,
    d DATE NOT NULL,
    chat_msgs INT DEFAULT 0,
    call_seconds INT DEFAULT 0,
    presence_seconds INT DEFAULT 0,
    PRIMARY KEY (user_id, d)
);
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
        # Close previous open of same kind, if different day:
        await con.execute("""
            UPDATE sessions SET end_at=now(), last_activity=now()
            WHERE user_id=$1 AND kind=$2 AND end_at IS NULL AND start_date<>$3
        """, user_id, kind, d)
        # Create new:
        await con.execute("""
            INSERT INTO sessions(user_id, kind, start_at, last_activity, start_date, source)
            VALUES($1,$2,$3,$3,$4,$5)
        """, user_id, kind, t, d, source or "")

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

        # برای کاندیدهای ادمینی (اعضای عادی هم ذخیره می‌شوند)
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
        b.button(text="✅ ثبت ورود کال", callback_data=f"ci:call:{user_id}")
    b.button(text="❌ ثبت خروج", callback_data=f"co:{kind}:{user_id}")
    b.adjust(1,1)
    return b.as_markup()


def kb_feedback(target_user_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="👍 راضی", callback_data=f"fb:{target_user_id}:1")
    b.button(text="👎 ناراضی", callback_data=f"fb:{target_user_id}:-1")
    b.adjust(2)
    return b.as_markup()


def kb_admin_panel(role: str, is_owner=False, is_senior_chat=False, is_senior_call=False, is_senior_all=False):
    b = InlineKeyboardBuilder()
    b.button(text="📊 آمار من", callback_data="pv:me")
    b.button(text="📈 آمار کلی من", callback_data="pv:me_all")
    b.button(text="✉️ ارتباط با مالک", callback_data="pv:contact_owner")
    b.button(text="📣 پیام به گارد", callback_data="pv:contact_guard")
    b.button(text="🚨 گزارش کاربر", callback_data="pv:report_user")

    if is_senior_chat or is_senior_all:
        b.button(text="🧑‍💻 لیست ادمین‌های چت", callback_data="pv:list_admins_chat")
        b.button(text="📝 ارسال پیام به گروه", callback_data="pv:send_to_main")
        b.button(text="📮 ارسال گزارش به مالک", callback_data="pv:send_report_owner")
        b.button(text="🚨 گزارش ادمین چت به مالک", callback_data="pv:report_admin_chat")

    if is_senior_call or is_senior_all:
        b.button(text="🎙️ لیست ادمین‌های کال", callback_data="pv:list_admins_call")
        b.button(text="📝 پیام به گروه (کال)", callback_data="pv:send_to_main_call")
        b.button(text="📮 گزارش به مالک (کال)", callback_data="pv:send_report_owner_call")
        b.button(text="🚨 گزارش ادمین کال به مالک", callback_data="pv:report_admin_call")

    if is_owner or is_senior_all:
        pass  # مالک ابزارهای متنی بدون / دارد؛ از PV فقط نمایش

    b.adjust(2)
    return b.as_markup()

# ----------------------------- Bot Init --------------------------------------
# ✅ FIX: use DefaultBotProperties for default parse_mode in aiogram >= 3.7
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TEHRAN)

pool: asyncpg.Pool = None
tclient: "TelegramClient|None" = None

# ---------------------------- Startup ----------------------------------------
async def on_startup():
    global pool, tclient
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as con:
        await con.execute(SCHEMA_SQL, MAIN_CHAT_ID, GUARD_CHAT_ID)
    log.info("DB ready.")

    if ENABLE_TELETHON and 'API_ID' in globals() and 'API_HASH' in globals() and 'TELETHON_SESSION' in globals() and API_ID and API_HASH and TELETHON_SESSION:
        tclient = TelegramClient(StringSession(TELETHON_SESSION), API_ID, API_HASH)
        await tclient.start()
        log.info("Telethon userbot started.")

        @tclient.on(events.Raw)
        async def telethon_raw_handler(event):
            if event.__class__.__name__ == "UpdateGroupCallParticipants":
                try:
                    pass  # TODO: implement accurate call->chat mapping if needed
                except Exception as e:
                    log.warning(f"Telethon handler error: {e}")

    # Close idle chat sessions after 10 minutes of inactivity (every minute)
    scheduler.add_job(job_autoclose_inactive_chat, CronTrigger.from_crontab("*/1 * * * *"))
    # Fallback: close stale call sessions (manual mode) if no heartbeat 10 min
    scheduler.add_job(job_autoclose_inactive_call_fallback, CronTrigger.from_crontab("*/1 * * * *"))
    # Daily close & report at 00:00 Tehran
    scheduler.add_job(job_daily_rollover, CronTrigger(hour=0, minute=0))
    scheduler.start()
    log.info("Scheduler started.")

# Register startup handler in aiogram 3.x safe way
from aiogram import Router

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
                # Notify guard & owner
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

async def job_daily_rollover():
    try:
        rows = await admins_overview_today(pool)
        if not rows: return
        lines = ["📊 <b>آمار امروز ادمین‌ها</b>\n(از ۰۰:۰۰ تا اکنون به وقت تهران)\n"]
        rows_sorted = sorted(rows, key=lambda r: (ROLE_ORDER.get(r["role"], 999), -r["msgs"], -r["call_secs"]))
        for r in rows_sorted:
            name = r["first_name"] or ""
            un = f"@{r['username']}" if r["username"] else ""
            rt = role_title(r["role"])
            lines.append(
                f"{rt} — <a href=\"tg://user?id={r['user_id']}\">{name or r['user_id']}</a> {un}\n"
                f"• پیام‌ها: <b>{r['msgs']}</b> | چت: <b>{pretty_td(r['chat_secs'])}</b> | کال: <b>{pretty_td(r['call_secs'])}</b>"
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
                clines.append(f"{rank}. <a href=\"tg://user?id={c['user_id']}\">{nm or c['user_id']}</a> {un} — "
                              f"چت: {c['chat_msgs']} | کال: {pretty_td(c['call_seconds'])} | حضور: {pretty_td(c['presence_seconds'])}")
                rank += 1
            await bot.send_message(OWNER_ID, "\n".join(clines))

    except Exception as e:
        log.error(f"job_daily_rollover: {e}")

# ------------------------------ Handlers -------------------------------------

# /start در PV
@CommandStart()
async def _start_filter(message: Message):
    return True

@dp.message(CommandStart(), F.chat.type == ChatType.PRIVATE)
async def start_pv(msg: Message):
    await ensure_user(pool, msg.from_user)
    role = await get_role(pool, msg.from_user.id)
    if is_admin_role(role):
        await msg.answer(
            "به پنل گارد سولز خوش آمدید.\nاز دکمه‌ها استفاده کنید:",
            reply_markup=kb_admin_panel(
                role,
                is_owner=(role=="owner" or msg.from_user.id==OWNER_ID),
                is_senior_chat=(role in {"senior_chat","senior_all"}),
                is_senior_call=(role in {"senior_call","senior_all"}),
                is_senior_all=(role=="senior_all")
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

    # اگر ادمین چت یا ارشد/مالک است پیشنهاد ثبت ورود
    if role in {"owner","senior_all","senior_chat","admin_chat"}:
        if await count_open(pool, u.id, "chat") == 0:
            await msg.reply(
                f"اولین پیام امروز ثبت شد. {u.first_name} عزیز، ورود/خروج چت را ثبت کنید:",
                reply_markup=kb_checkin("chat", u.id)
            )
        else:
            await touch_activity(pool, u.id, "chat")

# کال: چون Bot API ورود/خروج کال را نمی‌فهمد، دکمه دستی در گروه گارد:
@dp.message(F.chat.id == GUARD_CHAT_ID, F.text.lower().in_({"ثبت ورود کال","ثبت خروج کال"}))
async def guard_group_call_buttons_text(msg: Message):
    u = msg.from_user
    await ensure_user(pool, u)
    role = await get_role(pool, u.id)
    if role not in {"owner","senior_all","senior_call","admin_call"}:
        return
    if msg.text == "ثبت ورود کال":
        await open_session(pool, u.id, "call", source="manual")
        CALL_HEARTBEATS[u.id] = now_teh()
        await msg.reply("✅ ورود کال ثبت شد. (در صورت بی‌فعالی ۱۰ دقیقه‌ای خروج خودکار می‌خوری)")
        await bot.send_message(OWNER_ID, f"🎙️ ورود کال: <a href=\"tg://user?id={u.id}\">{u.first_name}</a>")
    else:
        await close_session(pool, u.id, "call")
        CALL_HEARTBEATS.pop(u.id, None)
        await msg.reply("⏹️ خروج کال ثبت شد.")
        await bot.send_message(OWNER_ID, f"🎙️ خروج کال: <a href=\"tg://user?id={u.id}\">{u.first_name}</a>")

# کال: دکمه شیشه‌ای (اگر خواستید در PV هم بدهید)
@dp.message(F.chat.id == MAIN_CHAT_ID, F.text.lower() == "کال")
async def main_group_call_help(msg: Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ثبت ورود کال", callback_data=f"ci:call:{msg.from_user.id}")
    kb.button(text="❌ ثبت خروج کال", callback_data=f"co:call:{msg.from_user.id}")
    kb.adjust(2)
    await msg.reply("برای کال یکی از گزینه‌ها را بزن:", reply_markup=kb.as_markup())

# کال‌بک‌های ورود/خروج
@dp.callback_query(F.data.regexp(r"^(ci|co):(chat|call):(\d+)$"))
async def cb_checkin_out(cb: CallbackQuery):
    action, kind, uid = cb.data.split(":")
    uid = int(uid)
    if cb.from_user.id != uid and cb.from_user.id != OWNER_ID:
        return await cb.answer("این دکمه مخصوص همان کاربر/مالک است.", show_alert=True)
    await ensure_user(pool, cb.from_user)

    if action == "ci":
        if await count_open(pool, uid, kind) > 0:
            await cb.answer("سشن باز داری.", show_alert=True); return
        await open_session(pool, uid, kind, source="inline")
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

# پنل PV دکمه‌ها
@dp.callback_query(F.data.startswith("pv:"))
async def pv_buttons(cb: CallbackQuery):
    await ensure_user(pool, cb.from_user)
    role = await get_role(pool, cb.from_user.id)

    if cb.data == "pv:me":
        st = await admin_today_stats(pool, cb.from_user.id)
        if st:
            txt = (f"📊 <b>آمار امروز شما</b>\n"
                   f"پیام‌ها: <b>{st['msgs']}</b>\n"
                   f"ریپلای‌ها (ارسال/دریافت): <b>{st['r_sent']}/{st['r_recv']}</b>\n"
                   f"زمان چت: <b>{pretty_td(st['chat_secs'])}</b>\n"
                   f"زمان کال: <b>{pretty_td(st['call_secs'])}</b>\n")
            await cb.message.edit_text(txt, reply_markup=kb_admin_panel(role,
                is_owner=(cb.from_user.id==OWNER_ID),
                is_senior_chat=(role in {"senior_chat","senior_all"}),
                is_senior_call=(role in {"senior_call","senior_all"}),
                is_senior_all=(role=="senior_all")
            ))
    elif cb.data == "pv:me_all":
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
               f"چت: <b>{pretty_td(st['chat_secs'])}</b> | کال: <b>{pretty_td(st['call_secs'])}</b>")
        await cb.message.edit_text(txt, reply_markup=kb_admin_panel(role,
            is_owner=(cb.from_user.id==OWNER_ID),
            is_senior_chat=(role in {"senior_chat","senior_all"}),
            is_senior_call=(role in {"senior_call","senior_all"}),
            is_senior_all=(role=="senior_all")
        ))
    elif cb.data == "pv:contact_owner":
        PENDING_CONTACT_OWNER.add(cb.from_user.id)
        await cb.message.edit_text("پیام‌تان به مالک را ارسال کنید (لغو: /cancel)")
    elif cb.data == "pv:contact_guard":
        PENDING_CONTACT_GUARD.add(cb.from_user.id)
        await cb.message.edit_text("پیام شما به گروه گارد ارسال می‌شود: الان متن را بفرستید. (لغو: /cancel)")
    elif cb.data == "pv:report_user":
        PENDING_REPORT[cb.from_user.id] = {"type": "member"}
        await cb.message.edit_text("آیدی عددی یا یوزرنیم کاربر را بفرستید.")
    elif cb.data == "pv:list_admins_chat":
        async with pool.acquire() as con:
            rows = await con.fetch("SELECT user_id, username, first_name, role FROM users WHERE role IN ('admin_chat','senior_chat','senior_all','owner') ORDER BY role")
        lines = ["🧑‍💻 ادمین‌های چت:"]
        for r in rows:
            lines.append(f"• {role_title(r['role'])}: <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a> @{r['username'] or ''}")
        await cb.message.edit_text("\n".join(lines))
    elif cb.data == "pv:list_admins_call":
        async with pool.acquire() as con:
            rows = await con.fetch("SELECT user_id, username, first_name, role FROM users WHERE role IN ('admin_call','senior_call','senior_all','owner') ORDER BY role")
        lines = ["🎙️ ادمین‌های کال:"]
        for r in rows:
            lines.append(f"• {role_title(r['role'])}: <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a> @{r['username'] or ''}")
        await cb.message.edit_text("\n".join(lines))
    elif cb.data in {"pv:send_to_main","pv:send_to_main_call","pv:send_report_owner","pv:send_report_owner_call","pv:report_admin_chat","pv:report_admin_call"}:
        await cb.message.edit_text("متن خود را ارسال کنید. (لغو: /cancel)")
        PENDING_REPORT[cb.from_user.id] = {"type": cb.data}
    await cb.answer()

# دریافت متن‌های پس از دکمه‌های PV
@dp.message(F.chat.type == ChatType.PRIVATE)
async def pv_text_flow(msg: Message):
    uid = msg.from_user.id
    if msg.text == "/cancel":
        PENDING_CONTACT_GUARD.discard(uid)
        PENDING_CONTACT_OWNER.discard(uid)
        PENDING_REPORT.pop(uid, None)
        return await msg.reply("لغو شد.", reply_markup=kb_admin_panel(await get_role(pool, uid)))

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
        typ = ctx["type"]
        if typ == "member":
            await bot.send_message(OWNER_ID, f"🚨 گزارش از <a href=\"tg://user?id={uid}\">{msg.from_user.first_name}</a>:\n{msg.text}")
            return await msg.reply("گزارش به مالک ارسال شد ✅")
        else:
            t = ctx["type"]
            if t == "pv:send_to_main":
                await bot.send_message(MAIN_CHAT_ID, f"📝 پیام از ارشد/ادمین:\n{msg.text}")
            elif t == "pv:send_to_main_call":
                await bot.send_message(MAIN_CHAT_ID, f"📝 [کال] پیام از ارشد/ادمین:\n{msg.text}")
            elif t == "pv:send_report_owner":
                await bot.send_message(OWNER_ID, f"📮 گزارش:\n{msg.text}")
            elif t == "pv:send_report_owner_call":
                await bot.send_message(OWNER_ID, f"📮 [کال] گزارش:\n{msg.text}")
            elif t == "pv:report_admin_chat":
                await bot.send_message(OWNER_ID, f"🚨 گزارش ادمین چت:\n{msg.text}")
            elif t == "pv:report_admin_call":
                await bot.send_message(OWNER_ID, f"🚨 گزارش ادمین کال:\n{msg.text}")
            return await msg.reply("انجام شد ✅")

# دکمه رضایت/نارضایتی (فقط مالک)
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
    (r"^(ترفیع|عزل)\s+(چت|کال|ارشدچت|ارشدکال|ارشدکل)\s+(@\w+|\d+)$", "promote_demote"),
    (r"^آمار\s*چت\s*الان$", "stats_chat_now"),
    (r"^آمار\s*کال\s*الان$", "stats_call_now"),
    (r"^آمار\s*$", "stats_active"),
    (r"^ممنوع\s+(\d+)$", "ban_user"),
    (r"^آزاد\s+(\d+)$", "unban_user"),
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
    text = (msg.text or "").strip()
    for pat, name in OWNER_CMD_PATTERNS:
        m = re.match(pat, text)
        if not m: continue
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
            role = ROLE_MAP[kind]
            if act == "ترفیع":
                await set_role(pool, target_id, role)
                await msg.reply(f"✅ {target_id} به {role_title(role)} ترفیع یافت.")
            else:
                await set_role(pool, target_id, "member")
                await msg.reply(f"✅ {target_id} عزل شد.")
            return
        elif name == "stats_chat_now":
            rows = await admins_overview_today(pool)
            lines = ["📊 آمار چت تا این لحظه:"]
            for r in sorted(rows, key=lambda r: ROLE_ORDER.get(r["role"], 99)):
                lines.append(f"{role_title(r['role'])} — <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a>: چت {pretty_td(r['chat_secs'])} | پیام {r['msgs']}")
            await msg.reply("\n".join(lines))
            return
        elif name == "stats_call_now":
            rows = await admins_overview_today(pool)
            lines = ["🎙️ آمار کال تا این لحظه:"]
            for r in sorted(rows, key=lambda r: ROLE_ORDER.get(r["role"], 99)):
                lines.append(f"{role_title(r['role'])} — <a href=\"tg://user?id={r['user_id']}\">{r['first_name'] or r['user_id']}</a>: کال {pretty_td(r['call_secs'])}")
            await msg.reply("\n".join(lines))
            return
        elif name == "stats_active":
            async with pool.acquire() as con:
                n = await con.fetchval("SELECT COUNT(DISTINCT user_id) FROM chat_metrics WHERE d=$1", today_teh())
            await msg.reply(f"👥 کاربران فعال امروز: <b>{n}</b>")
            return
        elif name == "ban_user":
            uid = int(m.group(1))
            async with pool.acquire() as con:
                await con.execute("INSERT INTO bans(user_id) VALUES($1) ON CONFLICT (user_id) DO NOTHING", uid)
            await msg.reply(f"⛔ کاربر {uid} در لیست ممنوع قرار گرفت.")
            return
        elif name == "unban_user":
            uid = int(m.group(1))
            async with pool.acquire() as con:
                await con.execute("DELETE FROM bans WHERE user_id=$1", uid)
            await msg.reply(f"✅ کاربر {uid} آزاد شد.")
            return
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
                    for uid in list(admin_ids)[:50]:
                        lines.append(f" - <a href=\"tg://user?id={uid}\">{uid}</a>")
                if commons:
                    lines.append("\n• اعضای مشترک:")
                    for uid in list(commons)[:100]:
                        lines.append(f" - <a href=\"tg://user?id={uid}\">{uid}</a>")
                await bot.send_message(GUARD_CHAT_ID, "\n".join(lines))
                await msg.reply("گزارش اتک ارسال شد.")
            except Exception as e:
                await msg.reply(f"خطا در اتک‌بک: {e}")
            return
        elif name == "call_title":
            title = m.group(1).strip()
            if not ENABLE_TELETHON or 'tclient' not in globals() or not tclient:
                return await msg.reply("تنظیم عنوان کال فقط با یوزربات (Telethon) ممکن است.")
            try:
                await msg.reply("(نمونه) درخواست تغییر عنوان کال ارسال شد. (پیاده‌سازی دقیق موردنیاز)")
            except Exception as e:
                await msg.reply(f"خطا: {e}")
            return
        elif name == "user_month":
            uid = int(m.group(1))
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
                """, uid, today_teh()-timedelta(days=30))
                role = await get_role(pool, uid)
                jg = await con.fetchval("SELECT joined_guard_at FROM users WHERE user_id=$1", uid)
            txt = (f"📚 آمار ۳۰ روز اخیر کاربر {uid} ({role_title(role)})\n"
                   f"پیام‌ها: {st['msgs']} | ریپلای‌ها: {st['rs']}/{st['rr']}\n"
                   f"چت: {pretty_td(st['chat_secs'])} | کال: {pretty_td(st['call_secs'])}\n"
                   f"تاریخ الحاق به گارد: {jg if jg else 'نامشخص'}")
            await msg.reply(txt, reply_markup=kb_feedback(uid))
            return
    # fallthrough

# ------------------------------ Misc -----------------------------------------
@dp.errors()
async def on_error(event, exception):
    log.error(f"Error: {exception}")

# ------------------------------- RUN -----------------------------------------
async def main():
    await dp.start_polling(bot, allowed_updates=["message","callback_query","chat_member","my_chat_member"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
