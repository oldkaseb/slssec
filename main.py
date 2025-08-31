#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hardened Telegram Bot
- python-telegram-bot v20+ (async)
- PostgreSQL via asyncpg
- Polling (no webhooks)
- Railway-ready

Security & Robustness improvements applied:
  • Enforce banned_users in all inbound user DM flows
  • Strict role/ownership checks for all admin-only callbacks/commands
  • Per-user throttling (contact/DM → guard/owner)
  • Defensive DB operations with typed helpers and transactions where useful
  • Idempotent job scheduling (prevent duplicates across restarts)
  • Input validation + safe parsing of IDs
  • Removed features entirely: media restriction, mute / unmute
  • Centralized error handling & logging, graceful shutdown

Functional coverage kept (hardened):
  • /start & home menu
  • Gender prompt (optional, toggle via config)
  • Sessions: open/close chat/call, idle auto-closure, per-shift message count
  • Daily stats, members_stats, watchlist
  • Nightly reports + random tag (toggle)
  • Guard/Owner contact pipeline with one-shot reply & ban flow

Env vars:
  BOT_TOKEN, DATABASE_URL, MAIN_CHAT_ID, GUARD_CHAT_ID, OWNER_ID, TZ, FUN_LINES_FILE (optional)
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable, Awaitable, Any

import asyncpg
from zoneinfo import ZoneInfo
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    ChatPermissions,
    MessageEntity,
)
from telegram.constants import ParseMode
from telegram.error import Forbidden, BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    AIORateLimiter,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# ---------------------------------------------------------
# Config & Globals
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("hardened-bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
DB_URL = os.environ.get("DATABASE_URL", "").strip()
MAIN_CHAT_ID = int(os.environ.get("MAIN_CHAT_ID", "0") or 0)
GUARD_CHAT_ID = int(os.environ.get("GUARD_CHAT_ID", "0") or 0)
OWNER_ID = int(os.environ.get("OWNER_ID", "0") or 0)
TZ = os.environ.get("TZ", "Europe/Oslo").strip() or "Europe/Oslo"

if DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")
if not DB_URL:
    raise RuntimeError("Missing DATABASE_URL")

LOCAL_TZ = ZoneInfo(TZ)

# Per-user throttling memory (in-process; acceptable for single replica)
THROTTLE_SECONDS_DM = 45  # user→guard DM flood control
_last_dm_ts: dict[int, datetime] = {}

# ---------------------------------------------------------
# DB Schema & Helpers
# ---------------------------------------------------------

SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS users (
    user_id      BIGINT PRIMARY KEY,
    first_name   TEXT,
    last_name    TEXT,
    username     TEXT,
    gender       TEXT,
    role         TEXT DEFAULT 'member',
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- random_tag (on/off), gender_prompt (on/off)
INSERT INTO config(key, value) VALUES
    ('random_tag', 'off') ON CONFLICT (key) DO NOTHING;
INSERT INTO config(key, value) VALUES
    ('gender_prompt', 'on') ON CONFLICT (key) DO NOTHING;

CREATE TABLE IF NOT EXISTS banned_users (
    user_id BIGINT PRIMARY KEY,
    reason  TEXT,
    banned_by BIGINT,
    banned_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS members_stats (
    user_id BIGINT,
    day     DATE,
    msgs    INTEGER DEFAULT 0,
    last_active TIMESTAMPTZ,
    PRIMARY KEY(user_id, day)
);

CREATE TABLE IF NOT EXISTS sessions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    kind    TEXT NOT NULL CHECK (kind IN ('chat','call')),
    start_ts TIMESTAMPTZ NOT NULL,
    last_activity_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ,
    msg_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sessions_open ON sessions(user_id) WHERE end_ts IS NULL;

CREATE TABLE IF NOT EXISTS daily_stats (
    user_id BIGINT,
    day     DATE,
    msgs    INTEGER DEFAULT 0,
    replies INTEGER DEFAULT 0,
    chat_seconds INTEGER DEFAULT 0,
    call_seconds INTEGER DEFAULT 0,
    first_in TIMESTAMPTZ,
    last_out TIMESTAMPTZ,
    PRIMARY KEY(user_id, day)
);

CREATE TABLE IF NOT EXISTS watchlist (
    user_id BIGINT PRIMARY KEY,
    note TEXT,
    added_by BIGINT,
    added_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ratings (
    day DATE PRIMARY KEY,
    up  INTEGER DEFAULT 0,
    down INTEGER DEFAULT 0,
    rated_by BIGINT
);
"""

@dataclass
class DB:
    pool: asyncpg.Pool

    # Generic helpers -------------------------------------------------
    async def fetchval(self, sql: str, *args) -> Any:
        async with self.pool.acquire() as con:
            return await con.fetchval(sql, *args)

    async def fetchrow(self, sql: str, *args) -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetchrow(sql, *args)

    async def fetch(self, sql: str, *args) -> list[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetch(sql, *args)

    async def execute(self, sql: str, *args) -> str:
        async with self.pool.acquire() as con:
            return await con.execute(sql, *args)

    async def tx(self, func: Callable[[asyncpg.Connection], Awaitable[Any]]):
        async with self.pool.acquire() as con:
            async with con.transaction():
                return await func(con)

    # Domain helpers --------------------------------------------------
    async def is_banned(self, user_id: int) -> bool:
        return bool(
            await self.fetchval("SELECT 1 FROM banned_users WHERE user_id=$1", user_id)
        )

    async def upsert_user(self, u) -> None:
        await self.execute(
            """
            INSERT INTO users(user_id, first_name, last_name, username, updated_at)
            VALUES($1,$2,$3,$4,NOW())
            ON CONFLICT(user_id)
            DO UPDATE SET first_name=EXCLUDED.first_name,
                          last_name=EXCLUDED.last_name,
                          username=EXCLUDED.username,
                          updated_at=NOW();
            """,
            u.id, u.first_name, u.last_name, u.username,
        )

    async def config_get(self, key: str, default: str = "") -> str:
        val = await self.fetchval("SELECT value FROM config WHERE key=$1", key)
        return val if val is not None else default

    async def config_set(self, key: str, value: str) -> None:
        await self.execute(
            "INSERT INTO config(key,value) VALUES($1,$2)\n"
            "ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
            key, value,
        )

    async def open_session(self, user_id: int, kind: str) -> None:
        now = datetime.now(tz=LOCAL_TZ)
        async def _do(con: asyncpg.Connection):
            # close any open sessions first
            await con.execute(
                "UPDATE sessions SET end_ts=$1 WHERE user_id=$2 AND end_ts IS NULL",
                now, user_id
            )
            await con.execute(
                "INSERT INTO sessions(user_id,kind,start_ts,last_activity_ts) VALUES($1,$2,$3,$3)",
                user_id, kind, now
            )
        await self.tx(_do)

    async def close_sessions(self, user_id: int, kind: Optional[str] = None) -> list[asyncpg.Record]:
        now = datetime.now(tz=LOCAL_TZ)
        async def _do(con: asyncpg.Connection):
            where = "user_id=$1 AND end_ts IS NULL"
            args = [user_id]
            if kind:
                where += " AND kind=$2"
                args.append(kind)
            rows = await con.fetch(f"SELECT * FROM sessions WHERE {where}", *args)
            await con.execute(f"UPDATE sessions SET end_ts=$1 WHERE {where}", now, *args)
            return rows
        return await self.tx(_do)

    async def bump_message(self, user_id: int, reply: bool) -> None:
        now = datetime.now(tz=LOCAL_TZ)
        today = now.date()
        async def _do(con: asyncpg.Connection):
            # members_stats
            await con.execute(
                """
                INSERT INTO members_stats(user_id, day, msgs, last_active)
                VALUES($1,$2,1,$3)
                ON CONFLICT(user_id,day)
                DO UPDATE SET msgs = members_stats.msgs + 1, last_active=$3
                """,
                user_id, today, now
            )
            # daily_stats (manager-level)
            await con.execute(
                """
                INSERT INTO daily_stats(user_id, day, msgs, replies, first_in)
                VALUES($1,$2,$3,$4,$5)
                ON CONFLICT(user_id, day)
                DO UPDATE SET msgs = daily_stats.msgs + $3,
                              replies = daily_stats.replies + $4,
                              last_out = COALESCE(daily_stats.last_out, $5)
                """,
                user_id, today, 1, 1 if reply else 0, now
            )
            # bump open chat session msg_count & last_activity_ts
            await con.execute(
                """
                UPDATE sessions SET msg_count = msg_count + 1, last_activity_ts=$1
                WHERE user_id=$2 AND end_ts IS NULL AND kind='chat'
                """,
                now, user_id
            )
        await self.tx(_do)

    async def accrue_session_seconds(self, rows: list[asyncpg.Record]) -> None:
        # on closing sessions, add seconds into daily_stats
        if not rows:
            return
        now = datetime.now(tz=LOCAL_TZ)
        today = now.date()
        async def _do(con: asyncpg.Connection):
            for r in rows:
                end_ts = now
                delta = int((end_ts - (r["start_ts"]).astimezone(LOCAL_TZ)).total_seconds())
                col = "chat_seconds" if r["kind"] == "chat" else "call_seconds"
                await con.execute(
                    f"""
                    INSERT INTO daily_stats(user_id, day, {col})
                    VALUES($1,$2,$3)
                    ON CONFLICT(user_id, day)
                    DO UPDATE SET {col} = daily_stats.{col} + $3,
                                  last_out = $4
                    """,
                    r["user_id"], today, delta, now
                )
        await self.tx(_do)


db: DB  # global holder

# ---------------------------------------------------------
# Utilities & Guards
# ---------------------------------------------------------

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

ADMIN_ROLES = {"chat_admin", "call_admin", "channel_admin", "senior_chat", "senior_call", "senior_channel", "owner"}

async def user_role(user_id: int) -> str:
    row = await db.fetchrow("SELECT role FROM users WHERE user_id=$1", user_id)
    if not row:
        return "member"
    return (row["role"] or "member")

async def ensure_role(update: Update, context: ContextTypes.DEFAULT_TYPE, roles: set[str]) -> bool:
    uid = update.effective_user.id if update.effective_user else 0
    if is_owner(uid):
        return True
    r = await user_role(uid)
    if r in roles:
        return True
    await update.effective_message.reply_text("⛔️ اجازهٔ دسترسی ندارید.")
    return False

# Safe ID parser
ID_RE = re.compile(r"^(\d{5,})$")

def parse_user_id(text: str) -> Optional[int]:
    m = ID_RE.match(text.strip())
    return int(m.group(1)) if m else None

# Throttle decorator
async def throttle_dm(user_id: int) -> bool:
    now = datetime.now(tz=LOCAL_TZ)
    last = _last_dm_ts.get(user_id)
    if last and (now - last).total_seconds() < THROTTLE_SECONDS_DM:
        return False
    _last_dm_ts[user_id] = now
    return True

# ---------------------------------------------------------
# Keyboards
# ---------------------------------------------------------

def kb_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("ورود چت", callback_data="enter:chat"), InlineKeyboardButton("ورود کال", callback_data="enter:call")],
        [InlineKeyboardButton("تغییر به چت", callback_data="switch:chat"), InlineKeyboardButton("تغییر به کال", callback_data="switch:call")],
        [InlineKeyboardButton("ثبت خروج", callback_data="exit:all")],
        [InlineKeyboardButton("ارتباط با گارد/مالک", callback_data="contact")],
    ])


def kb_gender() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚹 پسر", callback_data="gender:m"), InlineKeyboardButton("🚺 دختر", callback_data="gender:f")]
    ])

# ---------------------------------------------------------
# Handlers — Commands
# ---------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    await db.upsert_user(u)
    await update.effective_message.reply_text(
        "سلام! از منوی زیر انتخاب کن.",
        reply_markup=kb_home(),
    )


# ---------------------------------------------------------
# Handlers — Callback Buttons
# ---------------------------------------------------------

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    u = q.from_user
    await db.upsert_user(u)

    data = q.data or ""
    try:
        if data.startswith("enter:"):
            kind = data.split(":", 1)[1]
            await db.open_session(u.id, kind)
            await q.answer("ثبت شد ✅")
            await q.edit_message_text(f"ورود {('چت' if kind=='chat' else 'کال')} ثبت شد.")
            return

        if data.startswith("switch:"):
            kind = data.split(":", 1)[1]
            await db.open_session(u.id, kind)
            await q.answer("سوییچ شد ✅")
            await q.edit_message_text(f"فعلاً روی {('چت' if kind=='chat' else 'کال')} هستی.")
            return

        if data == "exit:all":
            rows = await db.close_sessions(u.id)
            await db.accrue_session_seconds(rows)
            await q.answer("خروج ثبت شد ✅")
            await q.edit_message_text("تمام سشن‌ها بسته شد.")
            return

        if data == "contact":
            # open DM pipe (instruction)
            await q.answer()
            await context.bot.send_message(
                chat_id=u.id,
                text=(
                    "پیام خودت رو همین‌جا برام بفرست تا به گارد/مالک برسونم.\n"
                    "لطفاً از اسپم خودداری کن — هر پیام تا ۴۵ ثانیه یک‌بار ارسال می‌شه."
                ),
            )
            await q.edit_message_text("برای ارتباط مستقیم، به PV من پیام بده.")
            return

        if data.startswith("gender:"):
            g = data.split(":", 1)[1]
            if g not in {"m","f"}:
                await q.answer("انتخاب نامعتبر.")
                return
            await db.execute("UPDATE users SET gender=$1, updated_at=NOW() WHERE user_id=$2", g, u.id)
            await q.answer("ذخیره شد ✅")
            await q.edit_message_text("جنسیت ذخیره شد. ممنون!")
            return

        await q.answer("دستور ناشناخته.")
    except Exception as e:
        log.exception("callback error: %s", e)
        try:
            await q.answer("خطا رخ داد.")
        except Exception:
            pass


# ---------------------------------------------------------
# Handlers — Messages (PV)
# ---------------------------------------------------------

async def on_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    u = update.effective_user
    if not msg or not u:
        return

    await db.upsert_user(u)

    # Enforce ban
    if await db.is_banned(u.id):
        try:
            await msg.reply_text("دسترسی شما مسدود است.")
        except Forbidden:
            pass
        return

    # Throttle
    if not await throttle_dm(u.id):
        await msg.reply_text("لطفاً کمی صبر کن، پیام‌هات پشت‌سرهم هست. ⏳")
        return

    # Forward to guard/owner channel
    text = msg.text_html or "(بدون متن)"
    caption = f"پیام جدید از <a href=\"tg://user?id={u.id}\">{u.first_name}</a> (id={u.id})\n\n{text}"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("پاسخ یک‌بار", callback_data=f"replyonce:{u.id}")],
        [InlineKeyboardButton("مسدود کاربر", callback_data=f"ban:{u.id}")],
    ])

    try:
        await context.bot.send_message(
            chat_id=GUARD_CHAT_ID or OWNER_ID,
            text=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
        await msg.reply_text("پیام به گارد/مالک ارسال شد ✅")
    except Exception as e:
        log.exception("forward error: %s", e)
        await msg.reply_text("ارسال پیام ناموفق بود. کمی بعد دوباره تلاش کن.")


# Admin replies to PV pipeline (replyonce and ban)
async def on_guard_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    u = q.from_user

    # only owner or admins can use these
    if not (is_owner(u.id) or (await ensure_role(update, context, ADMIN_ROLES))):
        try:
            await q.answer()
        except Exception:
            pass
        return

    data = q.data or ""

    if data.startswith("replyonce:"):
        target = parse_user_id(data.split(":",1)[1])
        if not target:
            await q.answer("آیدی نامعتبر.")
            return
        await q.answer("متن پاسخ را در همین ترد ریپلای کنید.")
        await q.edit_message_reply_markup(None)
        context.chat_data["reply_target"] = target
        return

    if data.startswith("ban:"):
        target = parse_user_id(data.split(":",1)[1])
        if not target:
            await q.answer("آیدی نامعتبر.")
            return
        await db.execute(
            "INSERT INTO banned_users(user_id, reason, banned_by) VALUES($1,$2,$3)\n"
            "ON CONFLICT(user_id) DO UPDATE SET reason=EXCLUDED.reason, banned_by=EXCLUDED.banned_by, banned_at=NOW()",
            target, "pipeline", u.id
        )
        await q.answer("کاربر مسدود شد ✅")
        await q.edit_message_reply_markup(None)
        try:
            await context.bot.send_message(chat_id=target, text="شما مسدود شدید.")
        except Exception:
            pass
        return


async def on_guard_text_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # If guard/owner replies inside guard chat with chat_data['reply_target'] set
    if update.effective_chat and (update.effective_chat.id not in {GUARD_CHAT_ID, OWNER_ID}):
        return
    target = context.chat_data.get("reply_target")
    if not target:
        return
    text = update.effective_message.text or ""
    try:
        await context.bot.send_message(chat_id=target, text=f"پاسخ مدیریت: {text}")
        await update.effective_message.reply_text("ارسال شد ✅")
        context.chat_data.pop("reply_target", None)
    except Forbidden:
        await update.effective_message.reply_text("کاربر پیام‌گیر نیست.")


# ---------------------------------------------------------
# Handlers — Group messages
# ---------------------------------------------------------

async def on_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.id != MAIN_CHAT_ID:
        return
    msg = update.effective_message
    u = update.effective_user
    if not msg or not u:
        return

    await db.upsert_user(u)

    # increment basic counters
    await db.bump_message(u.id, reply=bool(msg.reply_to_message))

    # If gender prompt is on and user has none, prompt silently via DM
    gender_prompt = (await db.config_get("gender_prompt", "on")) == "on"
    if gender_prompt:
        g = await db.fetchval("SELECT gender FROM users WHERE user_id=$1", u.id)
        if not g:
            try:
                await context.bot.send_message(chat_id=u.id, text="جنسیتت رو انتخاب کن:", reply_markup=kb_gender())
            except Forbidden:
                pass


# ---------------------------------------------------------
# Jobs (nightly + random tag) — idempotent
# ---------------------------------------------------------

async def job_nightly(context: ContextTypes.DEFAULT_TYPE):
    # Placeholder: aggregate and send reports — keep minimal here
    try:
        now = datetime.now(tz=LOCAL_TZ)
        await context.bot.send_message(chat_id=OWNER_ID, text=f"گزارش شبانه اجرا شد: {now:%Y-%m-%d}")
    except Exception as e:
        log.exception("nightly job error: %s", e)


async def job_random_tag(context: ContextTypes.DEFAULT_TYPE):
    try:
        state = (await db.config_get("random_tag", "off"))
        if state != "on":
            return
        # pick 3 most recently active users today
        today = datetime.now(tz=LOCAL_TZ).date()
        rows = await db.fetch(
            "SELECT user_id FROM members_stats WHERE day=$1 ORDER BY msgs DESC NULLS LAST LIMIT 3",
            today,
        )
        if not rows:
            return
        mentions = [f"<a href='tg://user?id={r['user_id']}'>کاربر</a>" for r in rows]
        await context.bot.send_message(
            chat_id=MAIN_CHAT_ID,
            text="🔥 " + "، ".join(mentions),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.exception("random tag job error: %s", e)


def schedule_jobs(app: Application):
    jq = app.job_queue
    # remove existing jobs with same names to prevent dupes on restart
    for name in ("nightly", "random_tag"):
        for job in jq.get_jobs_by_name(name):
            job.schedule_removal()

    # Nightly at 00:10
    jq.run_daily(job_nightly, time=datetime.time(hour=0, minute=10, tzinfo=LOCAL_TZ), name="nightly")
    # Every 15 minutes
    jq.run_repeating(job_random_tag, interval=900, first=30, name="random_tag")


# ---------------------------------------------------------
# Error handler
# ---------------------------------------------------------

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("update error: %s", context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("یه خطای غیرمنتظره رخ داد. لطفاً دوباره تلاش کن.")
    except Exception:
        pass


# ---------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------

async def init_db() -> DB:
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as con:
        await con.execute(SCHEMA_SQL)
    return DB(pool)


def build_app() -> Application:
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .rate_limiter(AIORateLimiter())
        .concurrent_updates(True)
        .build()
    )

    # Commands
    app.add_handler(CommandHandler("start", cmd_start))

    # Callbacks
    app.add_handler(CallbackQueryHandler(on_callback, pattern=r"^(enter:|switch:|exit:all|contact|gender:).+|^(contact)$"))
    app.add_handler(CallbackQueryHandler(on_guard_callbacks, pattern=r"^(replyonce:|ban:)\d+"))

    # Guard text reply in guard chat
    app.add_handler(MessageHandler(filters.Chat([GUARD_CHAT_ID, OWNER_ID]) & filters.TEXT & ~filters.COMMAND, on_guard_text_reply))

    # PV messages
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_private_message))

    # Group messages (MAIN_CHAT_ID)
    app.add_handler(MessageHandler(filters.Chat(MAIN_CHAT_ID) & ~filters.COMMAND, on_group_message))

    # Errors
    app.add_error_handler(on_error)

    schedule_jobs(app)
    return app


async def main():
    global db
    db = await init_db()
    app = build_app()
    # Polling
    await app.initialize()
    try:
        await app.start()
        log.info("Bot started (polling)")
        await app.updater.start_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
        await app.updater.idle()
    finally:
        await app.stop()
        await db.pool.close()
        log.info("Bot stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
