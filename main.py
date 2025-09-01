
# -*- coding: utf-8 -*-
"""
Souls / Souls Guard Telegram Bot
Single-file implementation for Railway + PostgreSQL.

Env vars expected in Railway:
- BOT_TOKEN        : Telegram bot token
- DATABASE_URL     : Postgres connection string (e.g. postgres://user:pass@host:port/dbname)
- MAIN_CHAT_ID     : int, ID of the main group (Souls)
- GUARD_CHAT_ID    : int, ID of the guard group (Souls Guard)
- OWNER_ID         : int, Telegram user id of the owner
- TZ               : e.g. Asia/Tehran (default if missing)

Notes:
- Turn OFF "Privacy mode" in BotFather to let the bot read all group messages for accurate stats.
- Add the bot as admin in both groups (at least: read, write, pin, delete messages recommended).
- Commands are plain-text Persian phrases (no slash).
"""

import os
import re
import json
import math
import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone

import pytz
import jdatetime
import asyncpg

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto,
    InputMediaVideo, InputMediaAudio, InputMediaDocument
)
from telegram.constants import ParseMode, ChatType
from telegram.ext import (
    ApplicationBuilder, ContextTypes, MessageHandler, filters,
    CallbackQueryHandler, ChatMemberHandler
)


# --- Owner-scope helpers for inline buttons (restrict glass buttons to the requester) ---
def with_owner(data: str, owner_id: int) -> str:
    return f"{data}|by:{owner_id}"

def split_owner_tag(data: str):
    # returns (core, owner_id or None)
    if "|by:" in data:
        core, tail = data.rsplit("|by:", 1)
        try:
            return core, int(tail)
        except Exception:
            return core, None
    return data, None

async def ensure_owner_or_alert(q, owner_id: int | None) -> bool:
    if owner_id is not None and q.from_user.id != owner_id:
        # Persian alert to match bot language
        await q.answer("این دکمه مخصوص درخواست‌کننده‌ست.", show_alert=True)
        return False
    return True
# -----------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# ENV & GLOBALS
# -------------------------------------------------------------------------------------

BOT_TOKEN    = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
MAIN_CHAT_ID = int(os.environ.get("MAIN_CHAT_ID", "0"))
GUARD_CHAT_ID= int(os.environ.get("GUARD_CHAT_ID", "0"))
OWNER_ID     = int(os.environ.get("OWNER_ID", "0"))
TZ_NAME      = os.environ.get("TZ", "Asia/Tehran")

TEHRAN_TZ = pytz.timezone(TZ_NAME)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
log = logging.getLogger("souls-bot")

# -------------------------------------------------------------------------------------
# DB
# -------------------------------------------------------------------------------------

POOL: asyncpg.Pool | None = None

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  gender TEXT DEFAULT 'unknown',
  joined_at TIMESTAMPTZ,
  last_seen TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS roles (
  user_id BIGINT,
  role TEXT,
  PRIMARY KEY (user_id, role)
);

CREATE TABLE IF NOT EXISTS bans (
  user_id BIGINT PRIMARY KEY,
  added_by BIGINT,
  reason TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sessions (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT,
  session_type TEXT, -- 'chat' | 'call'
  start_ts TIMESTAMPTZ,
  end_ts TIMESTAMPTZ,
  end_reason TEXT,
  active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS messages (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT,
  chat_id BIGINT,
  ts TIMESTAMPTZ,
  text TEXT,
  mention_count INT DEFAULT 0,
  has_media BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS toggles (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS contact_state (
  user_id BIGINT PRIMARY KEY,
  mode TEXT,           -- 'guard' | 'owner'
  can_send BOOLEAN,
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS reply_state (
  admin_id BIGINT PRIMARY KEY,
  target_user BIGINT,
  can_send BOOLEAN,
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS games (
  user_id BIGINT PRIMARY KEY,
  game TEXT,     -- game key
  state JSONB
);
"""

# -------------------------------------------------------------------------------------
# UTILITIES
# -------------------------------------------------------------------------------------

ROLES_ORDER = [
    "owner", "senior_all", "senior_chat", "senior_call", "admin_chat", "admin_call"
]

ROLE_DISPLAY = {
    "owner": "👑 مالک",
    "senior_all": "🛡 ارشد کل",
    "senior_chat": "🗨 ارشد چت",
    "senior_call": "📞 ارشد کال",
    "admin_chat": "💬 ادمین چت",
    "admin_call": "🎙 ادمین کال",
}

def is_persian_digits(s: str) -> bool:
    return bool(re.fullmatch(r"[۰-۹]+", s))

def persian_to_int(s: str) -> int:
    trans = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
    return int(s.translate(trans))

def to_jalali_str(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc).astimezone(TEHRAN_TZ)
    else:
        dt = dt.astimezone(TEHRAN_TZ)
    jdt = jdatetime.datetime.fromgregorian(datetime=dt)
    weekday = ["دوشنبه","سه‌شنبه","چهارشنبه","پنجشنبه","جمعه","شنبه","یکشنبه"][jdt.weekday()]  # jdt.weekday(): Mon=0
    return f"{jdt.year:04d}/{jdt.month:02d}/{jdt.day:02d} - {weekday}"

def human_td(seconds: int) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    parts = []
    if h: parts.append(f"{h}س")
    if m: parts.append(f"{m}د")
    if s and not h: parts.append(f"{s}ث")
    return " ".join(parts) if parts else "0"

async def db() -> asyncpg.Pool:
    global POOL
    if POOL is None:
        POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return POOL

async def init_db():
    pool = await db()
    async with pool.acquire() as con:
        await con.execute(CREATE_SQL)

async def upsert_user(u):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute(
            """INSERT INTO users(user_id, username, first_name, last_name, joined_at, last_seen)
               VALUES($1,$2,$3,$4,now(),now())
               ON CONFLICT (user_id) DO UPDATE SET
                   username=excluded.username,
                   first_name=excluded.first_name,
                   last_seen=now()""",
            u.id, u.username, u.first_name, u.last_name
        )

async def set_role(user_id: int, role: str, add: bool):
    pool = await db()
    async with pool.acquire() as con:
        if add:
            await con.execute("INSERT INTO roles(user_id, role) VALUES($1,$2) ON CONFLICT DO NOTHING", user_id, role)
        else:
            await con.execute("DELETE FROM roles WHERE user_id=$1 AND role=$2", user_id, role)

async def get_roles(user_id: int) -> list[str]:
    pool = await db()
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT role FROM roles WHERE user_id=$1", user_id)
        return [r["role"] for r in rows]

async def has_any_role(user_id: int, roles: list[str]) -> bool:
    if user_id == OWNER_ID:
        return True
    current = await get_roles(user_id)
    return any(r in current for r in roles)

async def add_ban(user_id: int, added_by: int, reason: str | None = None):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO bans(user_id, added_by, reason) VALUES($1,$2,$3) ON CONFLICT (user_id) DO NOTHING",
            user_id, added_by, reason
        )

async def remove_ban(user_id: int):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute("DELETE FROM bans WHERE user_id=$1", user_id)

async def is_banned(user_id: int) -> bool:
    pool = await db()
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT 1 FROM bans WHERE user_id=$1", user_id)
        return row is not None

async def record_message(user_id: int, chat_id: int, text: str | None, mention_count: int, has_media: bool, ts: datetime):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO messages(user_id, chat_id, ts, text, mention_count, has_media) VALUES($1,$2,$3,$4,$5,$6)",
            user_id, chat_id, ts, text, mention_count, has_media
        )

async def start_session(user_id: int, session_type: str) -> int:
    """Start new session; returns session id"""
    pool = await db()
    async with pool.acquire() as con:
        # close existing active
        await con.execute("UPDATE sessions SET active=FALSE, end_ts=now(), end_reason='override' WHERE user_id=$1 AND active", user_id)
        row = await con.fetchrow(
            "INSERT INTO sessions(user_id, session_type, start_ts, active) VALUES($1,$2,now(),TRUE) RETURNING id",
            user_id, session_type
        )
        return row["id"]

async def end_session_if_exists(user_id: int, reason: str = "manual"):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute(
            "UPDATE sessions SET active=FALSE, end_ts=now(), end_reason=$2 WHERE user_id=$1 AND active",
            user_id, reason
        )

async def active_session_type(user_id: int) -> str | None:
    pool = await db()
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT session_type FROM sessions WHERE user_id=$1 AND active", user_id)
        return row["session_type"] if row else None

async def daily_stats_for_date(date_from: datetime, date_to: datetime):
    """Return stats dict for admins between two timestamps"""
    pool = await db()
    async with pool.acquire() as con:
        msgs = await con.fetch("""
            SELECT user_id, COUNT(*) AS msg_count, COALESCE(SUM(mention_count),0) AS mentions
            FROM messages
            WHERE chat_id=$1 AND ts >= $2 AND ts < $3
            GROUP BY user_id
        """, MAIN_CHAT_ID, date_from, date_to)

        # session durations per type
        sess = await con.fetch("""
            SELECT user_id, session_type, SUM(EXTRACT(EPOCH FROM (LEAST(COALESCE(end_ts, now()), $3) - GREATEST(start_ts, $2)))) AS seconds
            FROM sessions
            WHERE start_ts < $3 AND COALESCE(end_ts, now()) > $2
            GROUP BY user_id, session_type
        """, date_from, date_to, date_to)

    msg_map = {r["user_id"]: {"msg": r["msg_count"], "men": r["mentions"]} for r in msgs}
    dur_map = {}
    for r in sess:
        d = dur_map.setdefault(r["user_id"], {"chat": 0, "call": 0})
        d[r["session_type"]] = int(r["seconds"] or 0)
    return msg_map, dur_map

# -------------------------------------------------------------------------------------
# INLINE UI BUILDERS
# -------------------------------------------------------------------------------------

def start_menu(owner_id: int):
    kb = [
        [InlineKeyboardButton("📨 تماس با گارد مدیران", callback_data=with_owner("contact:guard", owner_id))],
        [InlineKeyboardButton("👑 ارتباط با مالک", callback_data=with_owner("contact:owner", owner_id))],
        [InlineKeyboardButton("📊 آمار من", callback_data=with_owner("mystats", owner_id))]
    ]
    return InlineKeyboardMarkup(kb)

def contact_user_buttons(user_id: int):
    kb = [
        [InlineKeyboardButton("💬 پاسخ", callback_data=with_owner(f"guard_reply:{user_id}", owner_id)),
         InlineKeyboardButton("⛔ مسدود", callback_data=f"block:{user_id}")]
    ]
    return InlineKeyboardMarkup(kb)

def reply_again_buttons(user_id: int, owner_id: int):
    kb = [[InlineKeyboardButton("↩️ پاسخ مجدد", callback_data=with_owner(f"guard_reply:{user_id}", owner_id))]]
    return InlineKeyboardMarkup(kb)

def send_again_buttons(owner_id: int):
    kb = [[InlineKeyboardButton("📨 ارسال مجدد", callback_data=with_owner("send_again", owner_id))]]
    return InlineKeyboardMarkup(kb)

def session_choice_buttons(owner_id: int):
    kb = [[InlineKeyboardButton("🎙 کال", callback_data=with_owner("session:start:call", owner_id)),
           InlineKeyboardButton("💬 چت", callback_data=with_owner("session:start:chat", owner_id))]]
    return InlineKeyboardMarkup(kb)

def tag_panel(owner_id: int):
    kb = [
        [InlineKeyboardButton("🎙 تگ کال", callback_data=with_owner("tag:call", owner_id)),
         InlineKeyboardButton("💬 تگ چت", callback_data=with_owner("tag:chat", owner_id))],
        [InlineKeyboardButton("🔥 تگ اعضای فعال", callback_data=with_owner("tag:active", owner_id))],
        [InlineKeyboardButton("👧 تگ دخترها", callback_data=with_owner("tag:female", owner_id)),
         InlineKeyboardButton("👦 تگ پسرها", callback_data=with_owner("tag:male", owner_id))]
    ]
    return InlineKeyboardMarkup(kb)

def gender_panel(target_id: int, owner_id: int):
    kb = [[InlineKeyboardButton("👦 پسر", callback_data=with_owner(f"gender:{target_id}:male", owner_id)),
           InlineKeyboardButton("👧 دختر", callback_data=with_owner(f"gender:{target_id}:female", owner_id))]]
    return InlineKeyboardMarkup(kb)

def games_panel(owner_id: int):
    kb = [
        [InlineKeyboardButton("🎯 حدس عدد (۱۰۰)", callback_data=with_owner("game:number100", owner_id))],
        [InlineKeyboardButton("🎯 حدس عدد (۱۰۰۰)", callback_data=with_owner("game:number1000", owner_id))],
        [InlineKeyboardButton("🧠 حدس کلمه", callback_data=with_owner("game:word", owner_id))],
        [InlineKeyboardButton("🔤 اسکرامبل (درهم)", callback_data=with_owner("game:scramble", owner_id))],
        [InlineKeyboardButton("⌨️ تایپ سرعتی", callback_data=with_owner("game:typing", owner_id))],
        [InlineKeyboardButton("🧮 مسابقه حساب", callback_data=with_owner("game:math", owner_id))],
        [InlineKeyboardButton("🧩 اسم‌رمز اموجی", callback_data=with_owner("game:emoji", owner_id))],
        [InlineKeyboardButton("✂️ سنگ‌کاغذ‌قیچی", callback_data=with_owner("game:rps", owner_id))],
        [InlineKeyboardButton("🎲 دایس وار", callback_data=with_owner("game:dice", owner_id))],
        [InlineKeyboardButton("🪂 حدس حروف (هنگمن)", callback_data=with_owner("game:hangman", owner_id))],
        [InlineKeyboardButton("📚 معما", callback_data=with_owner("game:riddle", owner_id))],
        [InlineKeyboardButton("🔢 فرد/زوج", callback_data=with_owner("game:odd", owner_id))],
    ]
    return InlineKeyboardMarkup(kb)

# -------------------------------------------------------------------------------------
# FUN PHRASES (generated on the fly to reach 200+ without blowing the file size)
# -------------------------------------------------------------------------------------

def build_fun_pool():
    prefix = ["عه", "هی", "خب", "الو", "وای", "اووف", "عههه", "عه رفیق", "هی دادا", "هی بچه‌ها",
              "یالا", "سلام کجایی", "زِکی", "حاجی", "رفیق", "داش", "سلطان", "قربونت",
              "لِم دادی؟", "بیدار؟", "خبری نیست؟", "یخ زدی؟"]
    mid = ["کجایی", "بیدار شو", "پیدات نیست", "یه چیزی بگو", "بپر داخل", "حاضر شو",
           "یه سر بزن", "غیبت طولانی شد", "جواب بده", "بجنب", "سرِکارمون نذار",
           "چرا ساکتی", "پاتو بذار رو گاز", "حوصله سر رفت", "چایی آماده‌ست",
           "بیا ویس", "چت روشنه", "دلم برات تنگ شد", "صدام میاد؟", "اوضاع خوبيه؟"]
    suffix = ["😒", "😴", "😂", "🔥", "💥", "💤", "😎", "👀", "☕", "🎧", "📢", "🫶", "😜",
              "😐", "😑", "😈", "💣", "👻", "🚀", "🤌", "😅", "🥱", "🥳", "🤝", "🙃", "🤨"]
    pool = set()
    for a in prefix:
        for b in mid:
            for c in random.sample(suffix, k=min(4, len(suffix))):
                s = f"{a} {b} {c}"
                pool.add(s)
                if len(pool) >= 240:
                    return list(pool)
    return list(pool)

FUN_LINES = build_fun_pool()

def pretty_choice(pool):  # choose different each time
    return random.choice(pool)

ROBOT_NICE_LINES = [
    "به‌به! چه نورانی شدی امروز ✨",
    "سلطان، دمت گرم که اینجایی 🫶",
    "الهی فدات شم، حواسم بهت هست 😎",
    "قربون مرامت، مثِ همیشه خفنی 💪",
    "یا ابالفضل! حضور تو یعنی امنیت 😌",
    "آقا/خانم خاص! خوش اومدی 🌹",
]*20  # makes ~120 lines

# -------------------------------------------------------------------------------------
# PERMISSIONS
# -------------------------------------------------------------------------------------

async def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

async def is_manager(user_id: int) -> bool:
    if await is_owner(user_id):
        return True
    return await has_any_role(user_id, ["senior_all","senior_chat","senior_call","admin_chat","admin_call"])

async def is_senior_or_owner(user_id: int) -> bool:
    if await is_owner(user_id):
        return True
    return await has_any_role(user_id, ["senior_all","senior_chat","senior_call"])

# -------------------------------------------------------------------------------------
# CONTACT / BRIDGE
# -------------------------------------------------------------------------------------

async def ensure_contact_state(user_id: int, mode: str):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO contact_state(user_id, mode, can_send, updated_at) VALUES($1,$2,TRUE,now()) "
            "ON CONFLICT (user_id) DO UPDATE SET mode=excluded.mode, can_send=TRUE, updated_at=now()",
            user_id, mode
        )

async def can_user_send_contact(user_id: int) -> bool:
    pool = await db()
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT can_send FROM contact_state WHERE user_id=$1", user_id)
        return bool(row["can_send"]) if row else False

async def after_user_sent_contact(user_id: int):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute("UPDATE contact_state SET can_send=FALSE, updated_at=now() WHERE user_id=$1", user_id)

async def allow_user_send_again(user_id: int):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute("UPDATE contact_state SET can_send=TRUE, updated_at=now() WHERE user_id=$1", user_id)

async def set_reply_state(admin_id: int, target_user: int, allow=True):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO reply_state(admin_id, target_user, can_send, updated_at) VALUES($1,$2,$3,now()) "
            "ON CONFLICT (admin_id) DO UPDATE SET target_user=$2, can_send=$3, updated_at=now()",
            admin_id, target_user, allow
        )

async def get_reply_state(admin_id: int):
    pool = await db()
    async with pool.acquire() as con:
        return await con.fetchrow("SELECT target_user, can_send FROM reply_state WHERE admin_id=$1", admin_id)

async def disable_reply(admin_id: int):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute("UPDATE reply_state SET can_send=FALSE, updated_at=now() WHERE admin_id=$1", admin_id)

# -------------------------------------------------------------------------------------
# SCHEDULER JOBS
# -------------------------------------------------------------------------------------

async def job_poke_random(app):
    """Every 30 min: if toggle on → tag a random member who was active recently but silent lately."""
    try:
        if await get_toggle("poke_on", "0") != "1":
            return
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=24)
        silent = now - timedelta(hours=2)
        pool = await db()
        async with pool.acquire() as con:
            rows = await con.fetch("""
                WITH recent AS (
                    SELECT user_id, MAX(ts) last_ts
                    FROM messages
                    WHERE chat_id=$1 AND ts >= $2
                    GROUP BY user_id
                )
                SELECT r.user_id, u.first_name
                FROM recent r
                JOIN users u ON u.user_id=r.user_id
                WHERE r.last_ts < $3
                ORDER BY random()
                LIMIT 1
            """, MAIN_CHAT_ID, since, silent)
        if not rows:
            return
        row = rows[0]
        mention = f"<a href='tg://user?id={row['user_id']}'>{row['first_name'] or 'رفیق'}</a>"
        line = pretty_choice(FUN_LINES)
        await app.bot.send_message(MAIN_CHAT_ID, f"{mention} — {line}", parse_mode=ParseMode.HTML)
    except Exception as e:
        log.exception(e)

async def job_nightly_stats(app):
    """Send nightly stats to guard at 00:00 local time"""
    now_local = datetime.now(TEHRAN_TZ)
    day_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    day_end = day_start + timedelta(days=1)

    msg_map, dur_map = await daily_stats_for_date(day_start.astimezone(timezone.utc),
                                                  day_end.astimezone(timezone.utc))

    # Build two tables: chat admins + call admins
    async def roles_for(roles):
        pool = await db()
        async with pool.acquire() as con:
            rows = await con.fetch(
                "SELECT DISTINCT u.user_id, u.first_name, u.last_name FROM users u JOIN roles r ON u.user_id=r.user_id WHERE r.role = ANY($1)",
                roles
            )
            return rows

    jalali = to_jalali_str(day_start)
    lines_chat = [f"📊 آمار چت مدیران ({jalali})"]
    for row in await roles_for(["admin_chat", "senior_chat", "senior_all", "owner"]):
        uid = row["user_id"]
        m = msg_map.get(uid, {"msg":0,"men":0})
        d = dur_map.get(uid, {"chat":0,"call":0})
        lines_chat.append(f"• <a href='tg://user?id={uid}'>{row['first_name'] or 'کاربر'}</a> — پیام: {m['msg']} | منشن: {m['men']} | حضور چت: {human_td(d['chat'])}")

    lines_call = [f"🎙 آمار کال مدیران ({jalali})"]
    for row in await roles_for(["admin_call", "senior_call", "senior_all", "owner"]):
        uid = row["user_id"]
        d = dur_map.get(uid, {"chat":0,"call":0})
        lines_call.append(f"• <a href='tg://user?id={uid}'>{row['first_name'] or 'کاربر'}</a> — حضور کال: {human_td(d['call'])}")

    text = "\n".join(lines_chat + ["\n"] + lines_call)
    await app.bot.send_message(GUARD_CHAT_ID, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# -------------------------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------------------------

def parse_target_from_text(text: str) -> int | None:
    """Extract a numeric id from text (supports Persian digits)."""
    text = text.strip()
    m = re.search(r"(-?\d{6,})", text)
    if m:
        return int(m.group(1))
    # Persian digits
    m = re.search(r"(-?[۰-۹]{6,})", text)
    if m:
        return persian_to_int(m.group(1))
    return None

async def resolve_username_to_id(ctx: ContextTypes.DEFAULT_TYPE, username: str) -> int | None:
    try:
        if username.startswith("@"):
            username = username[1:]
        chat = await ctx.bot.get_chat(username)
        # Works if the username belongs to a user or a private chat
        return chat.id
    except Exception:
        return None
    except Exception:
        return None

def chunk_mentions(user_list: list[tuple[int,str]], n=5) -> list[str]:
    """Return list of lines each containing up to n inline mentions."""
    lines = []
    batch = []
    for uid, name in user_list:
        batch.append(f"<a href='tg://user?id={uid}'>{name}</a>")
        if len(batch) == n:
            lines.append(" ".join(batch))
            batch = []
    if batch:
        lines.append(" ".join(batch))
    return lines

# -------------------------------------------------------------------------------------
# HANDLERS
# -------------------------------------------------------------------------------------

async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await upsert_user(u)
    if update.effective_chat.type == ChatType.PRIVATE:
        await update.message.reply_html(
            "سلام! من ربات کمکی <b>Souls</b> هستم.\nاز منو یکی رو انتخاب کن:",
            reply_markup=start_menu(u.id)
        )

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    raw = q.data
    data, owner = split_owner_tag(raw)
    if not await ensure_owner_or_alert(q, owner):
        return

    if data.startswith("contact:"):
        mode = data.split(":",1)[1]
        await ensure_contact_state(q.from_user.id, mode)
        if mode == "guard":
            await q.message.edit_text(
                "پیامت رو همینجا بفرست. فقط اولین پیام منتقل میشه؛ برای پیام بعدی روی «ارسال مجدد» بزن.",
                reply_markup=send_again_buttons(q.from_user.id)
            )
        else:
            await q.message.edit_text(
                "در ارتباط با مالک هستی. اولین پیامت منتقل میشه؛ برای پیام بعدی «ارسال مجدد».",
                reply_markup=send_again_buttons(q.from_user.id)
            )
        return

    if data == "send_again":
        await allow_user_send_again(q.from_user.id)
        await q.message.reply_text("اوکی؛ پیام بعدی که بفرستی منتقل میشه.")
        return

    if data == "mystats":
        await send_user_stats(q.from_user.id, update, context)
        return

    if data.startswith("guard_reply:"):
        target = int(data.split(":")[1])
        await set_reply_state(q.from_user.id, target, allow=True)
        await q.message.reply_html(
            f"پاسخ به <a href='tg://user?id={target}'>کاربر</a> فعال شد. یک پیام بفرست.",
            reply_markup=reply_again_buttons(target, q.from_user.id)
        )
        return

    if data.startswith("block:"):
        uid = int(data.split(":")[1])
        if not await is_manager(q.from_user.id):
            await q.message.reply_text("اجازه نداری عزیز.")
            return
        await add_ban(uid, q.from_user.id, "blocked-from-bridge")
        # try to ban from main chat as well
        try:
            await context.bot.ban_chat_member(MAIN_CHAT_ID, uid)
        except Exception:
            pass
        await q.message.reply_text("کاربر به لیست ممنوع افزوده شد.")
        return

    if data.startswith("session:start:"):
        stype = data.split(":")[-1]
        await start_session(q.from_user.id, stype)
        mark = "✅ شروع فعالیت ثبت شد"
        await context.bot.send_message(GUARD_CHAT_ID,
            f"{mark} — {ROLE_DISPLAY.get('admin_'+('chat' if stype=='chat' else 'call'),'مدیر')} "
            f"<a href='tg://user?id={q.from_user.id}'>{q.from_user.first_name}</a>",
            parse_mode=ParseMode.HTML
        )
        await q.message.reply_text("شروع شد. موفق باشی 🌟")
        return

    if data.startswith("tag:"):
        _, kind = data.split(":")
        reply_to = q.message.reply_to_message.message_id if q.message and q.message.reply_to_message else None
        await do_tag(kind, update, context, reply_to=reply_to)
        return

    if data.startswith("gender:"):
        _, target, g = data.split(":")
        await set_gender(int(target), g, update, context)
        return

    if data.startswith("game:"):
        _, game_key = data.split(":")
        await start_game(game_key, update, context)
        return

# -----------------------------------------

async def bridge_from_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """When user is in contact mode and allowed, forward/copy the message to target (guard or owner)."""
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    user_id = update.effective_user.id
    pool = await db()
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT mode, can_send FROM contact_state WHERE user_id=$1", user_id)
    if not row or not row["can_send"]:
        return
    mode = row["mode"]
    target_chat = GUARD_CHAT_ID if mode == "guard" else OWNER_ID

    # For media-group (album)
    if update.message and update.message.media_group_id:
        # PTB already groups photos/video via MediaGroupHandler, but we can just forward copy.
        try:
            await update.message.copy(target_chat, caption=update.message.caption, caption_entities=update.message.caption_entities,
                                      reply_markup=contact_user_buttons(user_id) if mode=="guard" else None)
        except Exception as e:
            log.exception(e)
    else:
        try:
            await update.message.copy(target_chat, caption=update.message.caption, caption_entities=update.message.caption_entities,
                                      reply_markup=contact_user_buttons(user_id) if mode=="guard" else None)
        except Exception as e:
            log.exception(e)

    await after_user_sent_contact(user_id)
    await update.message.reply_text("پیامت منتقل شد ✔️", reply_markup=send_again_buttons(q.from_user.id))

async def guard_reply_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Listen in guard chat; if an admin has active reply_state, forward next message to target user once."""
    if update.effective_chat.id != GUARD_CHAT_ID:
        return
    admin_id = update.effective_user.id
    st = await get_reply_state(admin_id)
    if not st or not st["can_send"]:
        return
    target = st["target_user"]
    try:
        await update.message.copy(target)
        await context.bot.send_message(target, "📩 پاسخ گارد رسید.")
        await disable_reply(admin_id)
        await update.message.reply_text("پاسخ ارسال شد ✔️", reply_markup=reply_again_buttons(target, q.from_user.id))
    except Exception as e:
        log.exception(e)

# -----------------------------------------

async def on_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ban enforcement and user tracking on join/leave."""
    cm = update.chat_member
    user = cm.new_chat_member.user
    await upsert_user(user)
    if cm.chat.id != MAIN_CHAT_ID:
        return

    status = cm.new_chat_member.status
    if status in ("member", "administrator"):
        if await is_banned(user.id):
            try:
                await context.bot.ban_chat_member(MAIN_CHAT_ID, user.id)
                await context.bot.send_message(GUARD_CHAT_ID, f"⛔ کاربر ممنوع سعی کرد وارد شود: <a href='tg://user?id={user.id}'>{user.first_name}</a>", parse_mode=ParseMode.HTML)
            except Exception as e:
                log.exception(e)
    elif status in ("left", "kicked"):
        # optional: cleanup roles/gender when user leaves
        pool = await db()
        async with pool.acquire() as con:
            await con.execute("DELETE FROM roles WHERE user_id=$1", user.id)
        # gender left as history

# -----------------------------------------

INACTIVITY_MINUTES = 5
INACTIVITY_JOBS = {}  # user_id -> job

async def on_main_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Capture messages for stats & popup session start if manager without active session."""
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    msg = update.message
    u = msg.from_user
    await upsert_user(u)

    # record message for stats
    mention_count = 0
    if msg.entities:
        for e in msg.entities:
            if e.type in ("mention", "text_mention"):
                mention_count += 1
    has_media = any([msg.photo, msg.video, msg.audio, msg.document, msg.sticker, msg.voice, msg.animation])
    await record_message(u.id, MAIN_CHAT_ID, msg.text or msg.caption, mention_count, has_media, msg.date or datetime.now(timezone.utc))

    # if manager and no active session -> show popup
    if await is_manager(u.id):
        st = await active_session_type(u.id)
        if not st:
            try:
                await msg.reply_text("حضور رو ثبت کنیم؟", reply_markup=session_choice_buttons(u.id))
            except Exception:
                pass

        # setup inactivity end if has active session
        st = await active_session_type(u.id)
        if st:
            # reset job
            job = INACTIVITY_JOBS.get(u.id)
            if job:
                job.schedule_removal()
            job = context.job_queue.run_once(lambda c: asyncio.create_task(mark_inactive_timeout(u.id, context)),
                                             when=INACTIVITY_MINUTES*60)
            INACTIVITY_JOBS[u.id] = job

# -----------------------------------------

async def mark_inactive_timeout(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    await end_session_if_exists(user_id, reason="timeout")
    try:
        await context.bot.send_message(GUARD_CHAT_ID,
            f"❌ پایان فعالیت (عدم فعالیت) — <a href='tg://user?id={user_id}'>کاربر</a>", parse_mode=ParseMode.HTML)
    except Exception:
        pass

# -----------------------------------------
# TEXT COMMANDS (no slash)
# -----------------------------------------

async def on_text_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    u = update.effective_user
    chat_id = update.effective_chat.id

    # normalize spaces
    t = re.sub(r"\s+", " ", text)

    # owner-only quick fun switch
    if t in ("تگ خاموش", "خاموش تگ"):
        if not await is_owner(u.id):
            return
        await set_toggle("poke_on", "0")
        await update.message.reply_text("پینگ فان خاموش شد.")
        return
    if t in ("تگ روشن", "روشن تگ"):
        if not await is_owner(u.id):
            return
        await set_toggle("poke_on", "1")
        await update.message.reply_text("پینگ فان روشن شد.")
        return

    # help
    if t.startswith("راهنما"):
        await send_help(update, context)
        return

    # ban/unban (manager+)
    if t.startswith("ممنوع"):
        if not await is_manager(u.id):
            return
        target = await extract_target(update, context, t)
        if not target:
            await update.message.reply_text("هدف مشخص نیست.")
            return
        await add_ban(target, u.id, "manual")
        try:
            await context.bot.ban_chat_member(MAIN_CHAT_ID, target)
        except Exception:
            pass
        await update.message.reply_text("ثبت شد: کاربر ممنوع.")
        return

    if t.startswith("آزاد"):
        if not await is_manager(u.id):
            return
        target = await extract_target(update, context, t)
        if not target:
            await update.message.reply_text("هدف مشخص نیست.")
            return
        await remove_ban(target)
        try:
            await context.bot.unban_chat_member(MAIN_CHAT_ID, target, only_if_banned=True)
        except Exception:
            pass
        await update.message.reply_text("کاربر از لیست ممنوع خارج شد.")
        return

    # promotions/demotions (owner)
    if t.startswith("ترفیع"):
        if not await is_owner(u.id):
            return
        await do_promotion(update, context, t)
        return

    if t.startswith("عزل"):
        if not await is_owner(u.id):
            return
        await do_demotion(update, context, t)
        return

    # list guard
    if t.startswith("لیست گارد"):
        if not await is_senior_or_owner(u.id):
            return
        await list_guard(update, context)
        return

    # id stats
    if t in ("ایدی", "آیدی", "id"):
        await cmd_id(update, context)
        return

    # list bans
    if t.startswith("لیست ممنوع"):
        if not await is_manager(u.id):
            return
        await list_banned(update, context)
        return

    # tag open panel
    if t.startswith("تگ") and t == "تگ":
        await update.message.reply_text("کدوم دسته رو تگ کنم؟", reply_markup=tag_panel(u.id))
        return

    # gender
    if t.startswith("جنسیت"):
        if not await is_manager(u.id):
            return
        target = update.message.reply_to_message.from_user.id if update.message.reply_to_message else u.id
        await update.message.reply_text("جنسیت رو انتخاب کن:", reply_markup=gender_panel(target, u.id))
        return

    # session manual
    if t in ("ثبت", "ثبت حضور"):
        await update.message.reply_text("نوع فعالیت رو انتخاب کن:", reply_markup=session_choice_buttons(u.id))
        return
    if t in ("ثبت خروج", "پایان"):
        await end_session_if_exists(u.id, reason="manual")
        await update.message.reply_text("پایان فعالیت شما گزارش شد. خسته نباشید 🌙")
        try:
            await context.bot.send_message(GUARD_CHAT_ID,
                f"❎ پایان فعالیت — <a href='tg://user?id={u.id}'>{u.first_name}</a>",
                parse_mode=ParseMode.HTML)
        except Exception:
            pass
        return

    # games
    if t == "بازی":
        await update.message.reply_text("یک بازی انتخاب کن:", reply_markup=games_panel(u.id))
        return

    # robot (for managers)
    if t == "ربات":
        if not await is_manager(u.id):
            return
        await update.message.reply_text(pretty_choice(ROBOT_NICE_LINES))
        return

# -------- helper for target extraction

async def extract_target(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> int | None:
    # try reply target
    if update.message and update.message.reply_to_message:
        return update.message.reply_to_message.from_user.id
    # try username or id in text
    parts = text.split()
    if len(parts) >= 2:
        arg = parts[1]
        if arg.startswith("@"):
            uid = await resolve_username_to_id(context, arg)
            return uid
        if arg.lstrip("-").isdigit() or is_persian_digits(arg):
            return persian_to_int(arg) if is_persian_digits(arg) else int(arg)
    return None

# -------------------------------------------------------------------------------------
# HELP
# -------------------------------------------------------------------------------------

async def send_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owner_note = "✅ دستورات مالک از هرجا قابل اجراست."
    text = (
        "راهنما (خلاصه):\n"
        "• «ثبت» → شروع حضور (انتخاب چت/کال)\n"
        "• «ثبت خروج» → پایان حضور\n"
        "• «ممنوع [ریپلای/آیدی/یوزرنیم]» / «آزاد ...»\n"
        "• «ترفیع چت/کال/ارشد چت/ارشد کال/ارشد کل [هدف]»\n"
        "• «عزل چت/کال/ارشد چت/ارشد کال/ارشد کل [هدف]»\n"
        "• «لیست گارد» → نمایش مدیران به ترتیب سمت\n"
        "• «ایدی» یا «آیدی» → آمار ۷ روز گذشته (برای خودت یا با ریپلای برای دیگری)\n"
        "• «تگ» → پنل تگ (کال، چت، فعال‌ها، دخترها، پسرها)\n"
        "• «جنسیت» → پنل تعیین جنسیت (برای خودت یا با ریپلای)\n"
        "• «بازی» → پنل بازی‌ها\n"
        "• «ربات» → جمله قشنگ از ربات (فقط مقام‌دار)\n"
        f"\n{owner_note}\n"
        "یادآوری: حالت پرایوسی بات رو در BotFather خاموش کنید."
    )
    await update.message.reply_text(text)

# -------------------------------------------------------------------------------------
# PROMOTIONS / DEMOTIONS
# -------------------------------------------------------------------------------------

ROLE_KEYWORDS = {
    "چت": "admin_chat",
    "کال": "admin_call",
    "ارشد چت": "senior_chat",
    "ارشد کال": "senior_call",
    "ارشد کل": "senior_all",
}

async def do_promotion(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    # e.g. "ترفیع چت" with reply/arg
    role = None
    for k, v in ROLE_KEYWORDS.items():
        if f"ترفیع {k}" in text:
            role = v
            break
    if not role:
        await update.message.reply_text("سمت رو مشخص کن (مثلاً: «ترفیع چت»).")
        return
    target = await extract_target(update, context, text)
    if not target:
        await update.message.reply_text("هدف نامشخصه.")
        return
    await set_role(target, role, True)
    await update.message.reply_text("ترفیع انجام شد ✔️")

async def do_demotion(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    role = None
    for k, v in ROLE_KEYWORDS.items():
        if f"عزل {k}" in text:
            role = v
            break
    if not role:
        await update.message.reply_text("سمت رو مشخص کن (مثلاً: «عزل چت»).")
        return
    target = await extract_target(update, context, text)
    if not target:
        await update.message.reply_text("هدف نامشخصه.")
        return
    await set_role(target, role, False)
    await update.message.reply_text("عزل انجام شد ✔️")

# -------------------------------------------------------------------------------------
# LIST GUARD
# -------------------------------------------------------------------------------------

async def list_banned(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pool = await db()
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT user_id FROM bans ORDER BY created_at DESC LIMIT 100")
    if not rows:
        await update.message.reply_text("لیست ممنوع خالیه.")
        return
    users = []
    for r in rows:
        uid = r["user_id"]
        try:
            member = await context.bot.get_chat_member(MAIN_CHAT_ID, uid)
            name = member.user.first_name or "کاربر"
        except Exception:
            name = "کاربر"
        users.append((uid, name))
    lines = chunk_mentions(users, n=5)
    # Send to the current chat and also privately to owner (best effort)
    await update.message.reply_html("\n".join(lines))
    try:
        await context.bot.send_message(OWNER_ID, "لیست ممنوع:\n\n" + "\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception:
        pass

async def list_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pool = await db()
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT u.user_id, u.first_name, r.role
            FROM users u JOIN roles r ON u.user_id=r.user_id
        """)
    # order by role importance
    grouped = {r: [] for r in ROLES_ORDER}
    for row in rows:
        grouped[row["role"]].append((row["user_id"], row["first_name"] or "کاربر"))

    lines = []
    for role in ROLES_ORDER:
        if grouped[role]:
            role_name = ROLE_DISPLAY.get(role, role)
            lines.append(f"{role_name}:")
            lines += chunk_mentions(grouped[role])
            lines.append("")
    await update.message.reply_html("\n".join(lines) or "چیزی پیدا نشد.")

# -------------------------------------------------------------------------------------
# ID STATS (7-day)
# -------------------------------------------------------------------------------------

async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target = update.message.reply_to_message.from_user.id if update.message.reply_to_message else update.effective_user.id
    # gather 7-day stats
    now = datetime.now(TEHRAN_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    lines = []
    pool = await db()
    async with pool.acquire() as con:
        for i in range(7):
            start = (now - timedelta(days=i+1)).astimezone(timezone.utc)
            end = (now - timedelta(days=i)).astimezone(timezone.utc)
            mm = await con.fetchrow("""
                SELECT COUNT(*) c, COALESCE(SUM(mention_count),0) m FROM messages
                WHERE chat_id=$1 AND user_id=$2 AND ts >= $3 AND ts < $4
            """, MAIN_CHAT_ID, target, start, end)
            ss = await con.fetch("""
                SELECT session_type, SUM(EXTRACT(EPOCH FROM (LEAST(COALESCE(end_ts, now()), $3) - GREATEST(start_ts, $2)))) AS seconds
                FROM sessions WHERE user_id=$1 AND start_ts < $3 AND COALESCE(end_ts, now()) > $2 GROUP BY session_type
            """, target, start, end)
            dmap = {"chat":0,"call":0}
            for r in ss:
                dmap[r["session_type"]] = int(r["seconds"] or 0)
            daytxt = to_jalali_str((now - timedelta(days=i+1)))
            lines.append(f"{daytxt}\nپیام: {mm['c']} | منشن: {mm['m']} | چت: {human_td(dmap['chat'])} | کال: {human_td(dmap['call'])}")
    # photo (best effort)
    try:
        photos = await context.bot.get_user_profile_photos(target, limit=1)
        if photos.total_count > 0:
            fid = photos.photos[0][-1].file_id
            await update.message.reply_photo(fid, caption="\n\n".join(lines))
            return
    except Exception:
        pass
    await update.message.reply_text("\n\n".join(lines))

# -------------------------------------------------------------------------------------
# TAGGING
# -------------------------------------------------------------------------------------

async def do_tag(kind: str, update: Update, context: ContextTypes.DEFAULT_TYPE, reply_to: int | None = None):
    pool = await db()
    async with pool.acquire() as con:
        if kind == "call":
            rows = await con.fetch("SELECT u.user_id, u.first_name FROM users u JOIN roles r ON u.user_id=r.user_id WHERE r.role = ANY($1)",
                                   ["admin_call","senior_call","senior_all","owner"])
        elif kind == "chat":
            rows = await con.fetch("SELECT u.user_id, u.first_name FROM users u JOIN roles r ON u.user_id=r.user_id WHERE r.role = ANY($1)",
                                   ["admin_chat","senior_chat","senior_all","owner"])
        elif kind == "active":
            since = datetime.now(timezone.utc) - timedelta(days=2)
            rows = await con.fetch("""
                SELECT DISTINCT u.user_id, u.first_name FROM users u
                JOIN messages m ON u.user_id=m.user_id
                WHERE m.chat_id=$1 AND m.ts >= $2
                LIMIT 200
            """, MAIN_CHAT_ID, since)
        elif kind in ("male","female"):
            rows = await con.fetch("""SELECT user_id, first_name FROM users WHERE gender=$1""",
                                   ("male" if kind=="male" else "female"))
        else:
            rows = []
    data = [(r["user_id"], r["first_name"] or "کاربر") for r in rows]
    if not data:
        await context.bot.send_message(update.effective_chat.id, "کسی پیدا نشد.")
        return
    lines = chunk_mentions(data, n=5)
    for line in lines:
        await context.bot.send_message(update.effective_chat.id, line, parse_mode=ParseMode.HTML, reply_to_message_id=reply_to)

# -------------------------------------------------------------------------------------
# GENDER
# -------------------------------------------------------------------------------------

async def set_gender(user_id: int, gender: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute("UPDATE users SET gender=$2 WHERE user_id=$1", user_id, gender)
    await context.bot.send_message(update.effective_chat.id, "ثبت شد ✔️")

# -------------------------------------------------------------------------------------
# TOGGLES
# -------------------------------------------------------------------------------------

async def set_toggle(key: str, value: str):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute("INSERT INTO toggles(key,value) VALUES($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2", key, value)

async def get_toggle(key: str, default: str = "0") -> str:
    pool = await db()
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT value FROM toggles WHERE key=$1", key)
        return row["value"] if row else default

# -------------------------------------------------------------------------------------
# GAMES
# -------------------------------------------------------------------------------------

WORDS = ["سولز", "گارد", "مدیریت", "ربات", "تخفف", "پرچم", "هوش", "سیب", "مداد", "خلاقیت", "قهوه", "خیابان", "امروز", "خوشگل"]
EMOJI_CODE = {
    "🦁👑": "شیرشاه",
    "🌧️☔": "باران",
    "🔥📞": "کال داغ",
    "🧠⚡": "ایده",
}

RIDDLES = [
    ("اون چیه که هر چی برمیداری بزرگتر میشه؟", "چاله"),
    ("بدون نفس راه میره، بدون بال پرواز میکنه؟", "ابر"),
]

async def game_set(user_id: int, game: str, state: dict):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO games(user_id, game, state) VALUES($1,$2,$3) ON CONFLICT (user_id) DO UPDATE SET game=$2, state=$3",
            user_id, game, json.dumps(state, ensure_ascii=False)
        )

async def game_get(user_id: int):
    pool = await db()
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT game, state FROM games WHERE user_id=$1", user_id)
        if not row:
            return None, None
        return row["game"], json.loads(row["state"])

async def game_clear(user_id: int):
    pool = await db()
    async with pool.acquire() as con:
        await con.execute("DELETE FROM games WHERE user_id=$1", user_id)

async def start_game(game_key: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if game_key == "number100":
        target = random.randint(1, 100)
        await game_set(uid, "number", {"max":100, "target":target, "tries":0})
        await update.effective_message.reply_text("یه عدد بین ۱ تا ۱۰۰ حدس بزن.")
    elif game_key == "number1000":
        target = random.randint(1, 1000)
        await game_set(uid, "number", {"max":1000, "target":target, "tries":0})
        await update.effective_message.reply_text("یه عدد بین ۱ تا ۱۰۰۰ حدس بزن.")
    elif game_key == "word":
        word = random.choice(WORDS)
        tip = word[0] + ("‌" * (len(word)-1))
        await game_set(uid, "word", {"word":word, "tip":tip, "tries":0})
        await update.effective_message.reply_text(f"کلمه رو حدس بزن! نکته: {tip}")
    elif game_key == "scramble":
        word = random.choice(WORDS)
        scrambled = "".join(random.sample(list(word), len(word)))
        await game_set(uid, "scramble", {"word":word, "scrambled":scrambled})
        await update.effective_message.reply_text(f"حروف رو مرتب کن: {scrambled}")
    elif game_key == "typing":
        text = random.choice([
            "هوا بس ناجوانمردانه سرد است",
            "صداش کن بیاد اینجا",
            "رباتای خوب هیچوقت نمی‌خوابن",
            "گارد سولز همیشه بیداره"
        ])
        await game_set(uid, "typing", {"text":text, "start":datetime.now(timezone.utc).isoformat()})
        await update.effective_message.reply_text(f"این جمله رو دقیق و سریع تایپ کن:\n\n{text}")
    elif game_key == "math":
        a, b = random.randint(2, 30), random.randint(2, 30)
        op = random.choice(["+","-","*"])
        expr = f"{a} {op} {b}"
        ans = eval(expr.replace("x","*"))
        await game_set(uid, "math", {"expr":expr, "ans":ans})
        await update.effective_message.reply_text(f"حساب کن: {expr} = ?")
    elif game_key == "emoji":
        code, ans = random.choice(list(EMOJI_CODE.items()))
        await game_set(uid, "emoji", {"code":code, "ans":ans})
        await update.effective_message.reply_text(f"اسم‌رمز اموجی رو حدس بزن: {code}")
    elif game_key == "rps":
        await game_set(uid, "rps", {"await":True})
        await update.effective_message.reply_text("سنگ/کاغذ/قیچی رو بنویس.")
    elif game_key == "dice":
        you = random.randint(1,6)
        bot = random.randint(1,6)
        res = "بردی! 🎉" if you>bot else ("باختی 😅" if you<bot else "مساوی 😐")
        await update.effective_message.reply_text(f"تو: {you} | من: {bot} → {res}")
    elif game_key == "hangman":
        word = random.choice(WORDS)
        hidden = ["_" for _ in word]
        await game_set(uid, "hang", {"word":word, "shown":" ".join(hidden), "used":[], "left":6})
        await update.effective_message.reply_text(f"حدس حروف: { ' '.join(hidden) } (۶ فرصت)")
    elif game_key == "riddle":
        q,a = random.choice(RIDDLES)
        await game_set(uid, "riddle", {"ans":a})
        await update.effective_message.reply_text(q)
    elif game_key == "odd":
        n = random.randint(1, 30)
        await game_set(uid, "odd", {"n":n})
        await update.effective_message.reply_text(f"{n} فرده یا جفت؟ بنویس «فرد» یا «زوج».")
    else:
        await update.effective_message.reply_text("بازی پیدا نشد.")

async def games_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    game, state = await game_get(uid)
    if not game:
        return
    txt = (update.message.text or "").strip().lower()
    if game == "number":
        if txt.isdigit():
            g = int(txt)
            target = state["target"]
            state["tries"] += 1
            if g == target:
                await update.message.reply_text(f"بردی! تو {state['tries']} حدس👌")
                await game_clear(uid)
            elif g < target:
                await update.message.reply_text("بزرگ‌تره ↑")
                await game_set(uid, game, state)
            else:
                await update.message.reply_text("کوچیک‌تره ↓")
                await game_set(uid, game, state)
        return
    if game == "word":
        if txt == state["word"]:
            await update.message.reply_text("درست گفتی! 🎉")
            await game_clear(uid)
        else:
            await update.message.reply_text("نه! دوباره حدس بزن.")
        return
    if game == "scramble":
        if txt == state["word"]:
            await update.message.reply_text("ایول!")
            await game_clear(uid)
        else:
            await update.message.reply_text("نه، دوباره...")
        return
    if game == "typing":
        if txt == state["text"]:
            start = datetime.fromisoformat(state["start"])
            delta = datetime.now(timezone.utc) - start
            await update.message.reply_text(f"عالی! زمان: {round(delta.total_seconds(),2)} ثانیه")
            await game_clear(uid)
        else:
            await update.message.reply_text("غلط شد! دوباره سعی کن.")
        return
    if game == "math":
        try:
            val = int(txt)
        except Exception:
            return
        if val == state["ans"]:
            await update.message.reply_text("صحیح 👌")
            await game_clear(uid)
        else:
            await update.message.reply_text("نه!")
        return
    if game == "emoji":
        if state["ans"] in txt:
            await update.message.reply_text("درست گفتی!")
            await game_clear(uid)
        else:
            await update.message.reply_text("نچ 🙃")
        return
    if game == "rps":
        mapping = {"سنگ":0,"کاغذ":1,"قیچی":2}
        if txt not in mapping:
            return
        b = random.choice(list(mapping.keys()))
        you = mapping[txt]; bot = mapping[b]
        res = "بردی 🎉" if (you-bot)%3==1 else ("باختی 😅" if you!=bot else "مساوی 😐")
        await update.message.reply_text(f"تو: {txt} | من: {b} → {res}")
        await game_clear(uid)
        return
    if game == "hang":
        if len(txt) == 1:
            ch = txt
            if ch in state["used"]:
                await update.message.reply_text("این حرف رو زدی قبلاً!")
                return
            state["used"].append(ch)
            word = state["word"]
            shown = list(state["shown"].replace(" ",""))
            found = False
            for i, c in enumerate(word):
                if c == ch:
                    shown[i] = ch; found=True
            if not found:
                state["left"] -= 1
            new_shown = " ".join(shown)
            state["shown"] = new_shown
            if "_" not in shown:
                await update.message.reply_text(f"بردی! {word}")
                await game_clear(uid); return
            if state["left"] <= 0:
                await update.message.reply_text(f"باختی! کلمه: {word}")
                await game_clear(uid); return
            await update.message.reply_text(f"{new_shown} ({state['left']} فرصت)")
            await game_set(uid, "hang", state)
        else:
            if txt == state["word"]:
                await update.message.reply_text("خوبه! درست بود.")
                await game_clear(uid)
            else:
                state["left"] -= 2
                if state["left"] <= 0:
                    await update.message.reply_text(f"باختی! کلمه: {state['word']}")
                    await game_clear(uid)
                else:
                    await update.message.reply_text(f"نشد! ({state['left']} فرصت)")
                    await game_set(uid, "hang", state)
        return
    if game == "riddle":
        if state["ans"] in txt:
            await update.message.reply_text("درسته!")
            await game_clear(uid)
        else:
            await update.message.reply_text("نخیر!")
        return
    if game == "odd":
        if "فرد" in txt or "زوج" in txt:
            res = "فرد" if state["n"]%2==1 else "زوج"
            await update.message.reply_text(f"جواب: {res}")
            await game_clear(uid)
        return

# -------------------------------------------------------------------------------------
# NIGHTLY + STARTUP
# -------------------------------------------------------------------------------------

async def post_init(app):
    await init_db()
    # Schedule nightly stats at 00:00 local time
    now = datetime.now(TEHRAN_TZ)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    delay = (midnight - now).total_seconds()
    app.job_queue.run_repeating(lambda ctx: asyncio.create_task(job_nightly_stats(app)), interval=24*3600, first=delay)
    # Fun poke job: every 30 minutes
    app.job_queue.run_repeating(lambda ctx: asyncio.create_task(job_poke_random(app)), interval=30*60, first=60)

# -------------------------------------------------------------------------------------
# USER STATS FROM START MENU
# -------------------------------------------------------------------------------------

async def send_user_stats(user_id: int, update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(TEHRAN_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    lines = []
    pool = await db()
    async with pool.acquire() as con:
        for i in range(7):
            start = (now - timedelta(days=i+1)).astimezone(timezone.utc)
            end = (now - timedelta(days=i)).astimezone(timezone.utc)
            mm = await con.fetchrow("""SELECT COUNT(*) c, COALESCE(SUM(mention_count),0) m
                                       FROM messages WHERE chat_id=$1 AND user_id=$2 AND ts >= $3 AND ts < $4""",
                                    MAIN_CHAT_ID, user_id, start, end)
            ss = await con.fetch("""SELECT session_type, SUM(EXTRACT(EPOCH FROM (LEAST(COALESCE(end_ts, now()), $3) - GREATEST(start_ts, $2)))) AS seconds
                                    FROM sessions WHERE user_id=$1 AND start_ts < $3 AND COALESCE(end_ts, now()) > $2 GROUP BY session_type""",
                                    user_id, start, end)
            dmap = {"chat":0,"call":0}
            for r in ss:
                dmap[r["session_type"]] = int(r["seconds"] or 0)
            daytxt = to_jalali_str((now - timedelta(days=i+1)))
            lines.append(f"{daytxt}\nپیام: {mm['c']} | منشن: {mm['m']} | چت: {human_td(dmap['chat'])} | کال: {human_td(dmap['call'])}")
    await context.bot.send_message(update.effective_chat.id, "\n\n".join(lines))

# -------------------------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------------------------

def main():
    if not BOT_TOKEN or not DATABASE_URL or not MAIN_CHAT_ID or not GUARD_CHAT_ID or not OWNER_ID:
        raise SystemExit("One or more required env vars are missing.")
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # startup
    app.post_init = post_init

    # /start
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.Regex(r"^/start$"), on_start))
    app.add_handler(CallbackQueryHandler(on_callback))

    # Bridge from user PV
    # Any message in private chat considered for contact mode
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, bridge_from_user))

    # Guard reply
    app.add_handler(MessageHandler(filters.Chat(GUARD_CHAT_ID) & ~filters.COMMAND, guard_reply_listener))

    # Member updates in main chat
    app.add_handler(ChatMemberHandler(on_member_update, ChatMemberHandler.CHAT_MEMBER))

    # Main group messages for stats, popup, inactivity
    app.add_handler(MessageHandler(filters.Chat(MAIN_CHAT_ID) & ~filters.COMMAND, on_main_group_message))

    # Games input (works everywhere)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_command))

    # Games input (works everywhere)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, games_input))

    log.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
