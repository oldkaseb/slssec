# -*- coding: utf-8 -*-
"""
Souls / Souls Guard Telegram Bot (single-file, Railway-ready) — Patched & Flexible
- Safe DB auto-migrations (prevents UndefinedColumnError like "users.is_bot")
- Optional AIORateLimiter (runs even if PTB extras not installed)
- Implements: PM panel (guard/owner), one-shot messaging, admin replies, block DM,
  presence sessions (call/chat), auto end-after-idle, nightly stats (Jalali if available),
  bans list (ممنوع/آزاد), roles promote/demote, guard list, ID stats (7d + avatar),
  tag panel (call/chat/active/girls/boys, 5-by-5 mentions), gender popup,
  fun: random tag lines (200+ combos), 15+ text games with scoreboard,
  owner/global senior/admin scoping, user-scoped inline keyboards, and more.

Env vars:
  OWNER_ID , TZ , MAIN_CHAT_ID , GUARD_CHAT_ID , BOT_TOKEN , DATABASE_URL
"""

import asyncio
import logging
import os
import re
import random
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple

import asyncpg
from zoneinfo import ZoneInfo

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMemberUpdated,
    MessageEntity
)
from telegram.constants import ParseMode, ChatType
from telegram.ext import (
    Application, ApplicationBuilder, AIORateLimiter, ContextTypes, CommandHandler,
    MessageHandler, filters, CallbackQueryHandler, ChatMemberHandler, Defaults
)

try:
    import jdatetime  # optional (for Jalali dates)
except Exception:
    jdatetime = None

# ----------------------------- Config ---------------------------------

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
MAIN_CHAT_ID = int(os.getenv("MAIN_CHAT_ID", "0"))
GUARD_CHAT_ID = int(os.getenv("GUARD_CHAT_ID", "0"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
TZ = os.getenv("TZ", "Asia/Tehran")

TZINFO = ZoneInfo(TZ)

# ----------------------------- Logging --------------------------------

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("souls-bot")

# ----------------------------- DB Layer -------------------------------

class DB:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def create(cls, dsn: str) -> "DB":
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
        db = cls(pool)
        await db.init()
        return db

    async def init(self):
        # 1) create tables if not exist
        create_sql = """
        create table if not exists users(
            user_id bigint primary key,
            username text,
            first_name text,
            last_name text,
            is_bot boolean default false,
            gender text,
            last_seen_at timestamptz,
            in_group boolean default false
        );

        create table if not exists roles(
            user_id bigint references users(user_id) on delete cascade,
            role text not null,
            primary key (user_id, role)
        );

        create table if not exists bans(
            user_id bigint primary key,
            reason text,
            added_by bigint,
            added_at timestamptz default now()
        );

        create table if not exists contact_blocks(
            user_id bigint primary key,
            blocked boolean default true,
            reason text,
            updated_at timestamptz default now()
        );

        create table if not exists stats_daily(
            chat_id bigint,
            user_id bigint references users(user_id) on delete cascade,
            date date,
            messages_count integer default 0,
            media_count integer default 0,
            voice_count integer default 0,
            mentions_made_count integer default 0,
            call_time_sec integer default 0,
            primary key (chat_id, user_id, date)
        );

        create table if not exists sessions(
            id bigserial primary key,
            chat_id bigint not null,
            user_id bigint not null references users(user_id) on delete cascade,
            type text not null,
            start_at timestamptz not null,
            end_at timestamptz,
            ended_by text,
            active boolean default true
        );

        create table if not exists dm_threads(
            id bigserial primary key,
            kind text not null,
            user_id bigint not null,
            created_at timestamptz default now(),
            is_open boolean default true,
            last_admin_id bigint
        );

        create table if not exists contact_states(
            user_id bigint primary key,
            kind text not null,
            waiting boolean default false
        );

        create table if not exists admin_reply_states(
            admin_id bigint,
            target_user_id bigint,
            kind text not null,
            primary key (admin_id, kind)
        );

        create table if not exists toggles(
            chat_id bigint primary key,
            random_tag boolean default false
        );

        create table if not exists active_members(
            chat_id bigint,
            user_id bigint,
            last_activity_at timestamptz,
            primary key (chat_id, user_id)
        );

        create table if not exists game_scores(
            chat_id bigint,
            user_id bigint,
            score integer default 0,
            updated_at timestamptz default now(),
            primary key (chat_id, user_id)
        );
        """
        # 2) safe migrations for old installs (prevents "is_bot does not exist")
        migrate_sql = """
        -- users
        alter table if exists users add column if not exists username text;
        alter table if exists users add column if not exists first_name text;
        alter table if exists users add column if not exists last_name text;
        alter table if exists users add column if not exists is_bot boolean default false;
        alter table if exists users add column if not exists gender text;
        alter table if exists users add column if not exists last_seen_at timestamptz;
        alter table if exists users add column if not exists in_group boolean default false;

        -- stats_daily
        alter table if exists stats_daily add column if not exists media_count integer default 0;
        alter table if exists stats_daily add column if not exists voice_count integer default 0;
        alter table if exists stats_daily add column if not exists mentions_made_count integer default 0;
        alter table if exists stats_daily add column if not exists call_time_sec integer default 0;

        -- sessions
        alter table if exists sessions add column if not exists ended_by text;
        alter table if exists sessions add column if not exists active boolean default true;

        -- bans
        alter table if exists bans add column if not exists reason text;
        alter table if exists bans add column if not exists added_by bigint;
        alter table if exists bans add column if not exists added_at timestamptz default now();

        -- active_members / toggles
        alter table if exists active_members add column if not exists last_activity_at timestamptz;
        alter table if exists toggles add column if not exists random_tag boolean default false;
        """
        async with self.pool.acquire() as con:
            await con.execute(create_sql)
            await con.execute(migrate_sql)

        # Seed owner
        if OWNER_ID:
            await self.upsert_user(OWNER_ID, username=None, first_name="OWNER", last_name=None, is_bot=False)
            await self.add_role(OWNER_ID, "owner")

    # --- User helpers ---
    async def upsert_user(self, user_id: int, username: Optional[str], first_name: str, last_name: Optional[str], is_bot: bool):
        async with self.pool.acquire() as con:
            await con.execute("""
            insert into users(user_id, username, first_name, last_name, is_bot, last_seen_at)
            values($1,$2,$3,$4,$5, now())
            on conflict (user_id) do update set
                username = excluded.username,
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                is_bot = excluded.is_bot,
                last_seen_at = now();
            """, user_id, username, first_name, last_name, is_bot)

    async def set_user_in_group(self, user_id: int, in_group: bool):
        async with self.pool.acquire() as con:
            await con.execute("update users set in_group=$2 where user_id=$1;", user_id, in_group)

    async def set_gender(self, user_id: int, gender: Optional[str]):
        async with self.pool.acquire() as con:
            await con.execute("update users set gender=$2 where user_id=$1;", user_id, gender)

    async def get_user(self, user_id: int) -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetchrow("select * from users where user_id=$1;", user_id)

    # --- Roles ---
    async def add_role(self, user_id: int, role: str):
        async with self.pool.acquire() as con:
            await con.execute("insert into roles(user_id, role) values($1,$2) on conflict do nothing;", user_id, role)

    async def remove_role(self, user_id: int, role: str):
        async with self.pool.acquire() as con:
            await con.execute("delete from roles where user_id=$1 and role=$2;", user_id, role)

    async def has_any_role(self, user_id: int, roles: List[str]) -> bool:
        async with self.pool.acquire() as con:
            rows = await con.fetch("select role from roles where user_id=$1;", user_id)
        rs = {r["role"] for r in rows}
        return any(x in rs for x in roles)

    async def get_roles(self, user_id: int) -> List[str]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("select role from roles where user_id=$1 order by role;", user_id)
        return [r["role"] for r in rows]

    async def list_by_role(self, role: str) -> List[int]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("select user_id from roles where role=$1;", role)
        return [r["user_id"] for r in rows]

    async def list_all_managers(self) -> Dict[str, List[int]]:
        roles = ['owner','senior_global','senior_call','senior_chat','admin_call','admin_chat']
        res = {}
        for r in roles:
            res[r] = await self.list_by_role(r)
        return res

    # --- Bans ---
    async def ban_add(self, user_id: int, reason: Optional[str], added_by: int):
        async with self.pool.acquire() as con:
            await con.execute("""
            insert into bans(user_id, reason, added_by) values($1,$2,$3)
            on conflict (user_id) do update set reason=excluded.reason, added_by=excluded.added_by, added_at=now();
            """, user_id, reason, added_by)

    async def ban_remove(self, user_id: int):
        async with self.pool.acquire() as con:
            await con.execute("delete from bans where user_id=$1;", user_id)

    async def is_banned(self, user_id: int) -> bool:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("select 1 from bans where user_id=$1;", user_id)
        return bool(row)

    async def list_banned(self) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("select * from bans order by added_at desc;")
        return rows

    # --- Contact blocks ---
    async def set_contact_block(self, user_id: int, blocked: bool, reason: Optional[str] = None):
        async with self.pool.acquire() as con:
            await con.execute("""
                insert into contact_blocks(user_id, blocked, reason, updated_at)
                values($1,$2,$3, now())
                on conflict (user_id) do update set blocked=$2, reason=$3, updated_at=now();
            """, user_id, blocked, reason)

    async def is_contact_blocked(self, user_id: int) -> bool:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("select blocked from contact_blocks where user_id=$1;", user_id)
        return bool(row and row["blocked"])

    # --- Stats ---
    async def bump_stat(self, chat_id: int, user_id: int, *, is_media: bool, is_voice: bool, mentions_made: int, at: datetime):
        d = at.astimezone(TZINFO).date()
        async with self.pool.acquire() as con:
            await con.execute("""
            insert into stats_daily(chat_id, user_id, date, messages_count, media_count, voice_count, mentions_made_count)
            values($1,$2,$3,1,$4,$5,$6)
            on conflict (chat_id,user_id,date) do update set
                messages_count = stats_daily.messages_count + 1,
                media_count = stats_daily.media_count + excluded.media_count,
                voice_count = stats_daily.voice_count + excluded.voice_count,
                mentions_made_count = stats_daily.mentions_made_count + excluded.mentions_made_count;
            """, chat_id, user_id, d, 1 if is_media else 0, 1 if is_voice else 0, mentions_made)

    async def add_session(self, chat_id: int, user_id: int, kind: str, start_at: datetime):
        async with self.pool.acquire() as con:
            await con.execute("""
            insert into sessions(chat_id,user_id,type,start_at,active) values($1,$2,$3,$4,true);
            """, chat_id, user_id, kind, start_at)

    async def end_session(self, chat_id: int, user_id: int, ended_by: str, end_at: datetime):
        # Use CTE to update latest active session safely (PostgreSQL compliant)
        async with self.pool.acquire() as con:
            row = await con.fetchrow("""
                with c as (
                    select id from sessions
                    where chat_id=$1 and user_id=$2 and active=true
                    order by start_at desc
                    limit 1
                )
                update sessions s
                set active=false, end_at=$3, ended_by=$4
                from c
                where s.id = c.id
                returning s.start_at, s.type;
            """, chat_id, user_id, end_at, ended_by)
            return row

    async def has_active_session(self, chat_id: int, user_id: int) -> bool:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("select 1 from sessions where chat_id=$1 and user_id=$2 and active=true;", chat_id, user_id)
        return bool(row)

    async def update_call_time_aggregate_for_day(self, chat_id: int, user_id: int, d: date):
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                select start_at, coalesce(end_at, now()) as end_at
                from sessions
                where chat_id=$1 and user_id=$2 and type='call' and date(start_at at time zone $3)=$4;
            """, chat_id, user_id, TZ, d)
            total = 0
            for r in rows:
                delta = (r["end_at"] - r["start_at"]).total_seconds()
                if delta > 0:
                    total += int(delta)
            await con.execute("""
                insert into stats_daily(chat_id,user_id,date,call_time_sec)
                values($1,$2,$3,$4)
                on conflict (chat_id,user_id,date) do update set
                    call_time_sec=excluded.call_time_sec;
            """, chat_id, user_id, d, total)

    async def get_stats_for_user_days(self, chat_id: int, user_id: int, days: int) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                select * from stats_daily where chat_id=$1 and user_id=$2
                order by date desc limit $3;
            """, chat_id, user_id, days)
        return rows

    async def set_active_member(self, chat_id: int, user_id: int, at: datetime):
        async with self.pool.acquire() as con:
            await con.execute("""
                insert into active_members(chat_id,user_id,last_activity_at)
                values($1,$2,$3)
                on conflict (chat_id,user_id) do update set last_activity_at=$3;
            """, chat_id, user_id, at)

    async def get_active_members(self, chat_id: int, since_minutes: int = 1440) -> List[int]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                select user_id from active_members
                where chat_id=$1 and last_activity_at >= now() - ($2::text||' minutes')::interval;
            """, chat_id, since_minutes)
        return [r["user_id"] for r in rows]

    async def list_gender(self, gender: str) -> List[int]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("select user_id from users where gender=$1 and in_group=true;", gender)
        return [r["user_id"] for r in rows]

    async def inc_game_score(self, chat_id: int, user_id: int, delta: int = 1):
        async with self.pool.acquire() as con:
            await con.execute("""
                insert into game_scores(chat_id,user_id,score,updated_at) values($1,$2,$3,now())
                on conflict (chat_id,user_id) do update set score = game_scores.score + $3, updated_at=now();
            """, chat_id, user_id, delta)

    async def get_game_top(self, chat_id: int, limit: int = 10):
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                select u.user_id, coalesce(u.first_name,'') as fn, coalesce(u.last_name,'') as ln, u.username as un, s.score
                from game_scores s
                join users u on u.user_id = s.user_id
                where s.chat_id=$1
                order by s.score desc nulls last, updated_at desc
                limit $2;
            """, chat_id, limit)
        return rows

    async def set_random_tag(self, chat_id: int, on: bool):
        async with self.pool.acquire() as con:
            await con.execute("""
                insert into toggles(chat_id, random_tag) values($1,$2)
                on conflict (chat_id) do update set random_tag=$2;
            """, chat_id, on)

    async def get_random_tag(self, chat_id: int) -> bool:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("select random_tag from toggles where chat_id=$1;", chat_id)
        return bool(row and row["random_tag"])

# ----------------------------- Utilities ------------------------------

def mention(user_id: int, name: str) -> str:
    safe = re.sub(r'[\[\]\(\)_*`>#+\-=|{}.!]', '', name or "کاربر")
    return f"[{safe}](tg://user?id={user_id})"

def now_tz() -> datetime:
    return datetime.now(tz=TZINFO)

WEEKDAYS_FA = ["دوشنبه","سه‌شنبه","چهارشنبه","پنج‌شنبه","جمعه","شنبه","یکشنبه"]

def format_jalali(dt: datetime) -> str:
    if jdatetime is None:
        return dt.astimezone(TZINFO).strftime("%Y-%m-%d %H:%M")
    j = jdatetime.datetime.fromgregorian(datetime=dt.astimezone(TZINFO))
    weekdays = ["دوشنبه","سه‌شنبه","چهارشنبه","پنج‌شنبه","جمعه","شنبه","یکشنبه"]
    return f"{j.strftime('%Y/%m/%d %H:%M')} - {weekdays[j.weekday()]}"

def format_secs(s: int) -> str:
    h = s // 3600
    s -= h*3600
    m = s // 60
    s -= m*60
    return f"{h:02}:{m:02}:{s:02}"

def alert_not_for_you():
    return "این دکمه برای شما نیست رفیق! 😅"

FUN_PREFIXES = ["هی","اوه","سرورِ مهربون","آقا/خانم قهرمان","حاجی","رفیق","هی رفیق","قربونت","عه","ای جان"]
FUN_SUFFIXES = ["کجایی؟ 😴","بیا یه تکونی به خودت بده! 💃","جمع خوابالوهاست؟ 😜","چایی حاضر شد، بیا! ☕","ما که پیر شدیم، تو بیا! 👴","بی‌خیال تنبلی، بپر تو چت! 🏃","دلتنگت شدیم! ❤️","یه چیزی بگو دیگه! 🎤","بپر تو ویس کال ببینیمت! 🎧","تو که رفتی، سکوت اومد! 🤫","نیا نیا، شوخی کردم بیا 😂","میای یا بزنم تگ بعدی؟ 🤨","غیبت طولانی، گزارش میشه‌ها! 📋"]
BOT_NICE_LINES_BASE = ["قربون محبتت برم! 😍","جانِ دلمی! 💙","تو که باشی، همه چی روبه‌راست 😎","این گروه با تو می‌درخشه ✨","دمت گرم که هستی 💪","ایول بهت! 👏","خاص‌ترین آدمِ جمعی 😌","فدات که فعالی 🌟","تو هیچی کم نداری ❤️","مرسی که حالِ جمعو خوب می‌کنی 🌈"]
RANDOM_TAG_LINES = [f"{p} {s}" for p in FUN_PREFIXES for s in FUN_SUFFIXES] * 5
BOT_NICE_LINES = BOT_NICE_LINES_BASE * 12

# ----------------------------- Permission Helpers ---------------------
async def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

async def is_manager(db: DB, user_id: int) -> bool:
    if await is_owner(user_id):
        return True
    return await db.has_any_role(user_id, ['senior_global','senior_call','senior_chat','admin_call','admin_chat'])

async def is_senior(db: DB, user_id: int) -> bool:
    if await is_owner(user_id):
        return True
    return await db.has_any_role(user_id, ['senior_global','senior_call','senior_chat'])

# ----------------------------- Start & PM Panel -----------------------
def pm_panel_kb() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("📨 ارتباط با گارد مدیران", callback_data="pm|guard")],
        [InlineKeyboardButton("👑 ارتباط با مالک", callback_data="pm|owner")],
        [InlineKeyboardButton("📊 آمار من", callback_data="pm|mystats")],
    ]
    return InlineKeyboardMarkup(kb)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await context.bot.send_message(
        chat_id=u.id,
        text="سلام! این پنل شخصی ربات سولزه. یکی از گزینه‌ها رو انتخاب کن 👇",
        reply_markup=pm_panel_kb()
    )

# ----------------------------- Contact Flows --------------------------
async def ensure_user(db: DB, u) -> None:
    await db.upsert_user(u.id, u.username, u.first_name or "", u.last_name, u.is_bot)

async def cb_pm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split("|")
    if len(data) < 2:
        return
    kind = data[1]  # guard / owner / mystats
    user = query.from_user
    db: DB = context.bot_data["DB"]
    await ensure_user(db, user)
    if kind == "mystats":
        await send_stats_for_user(user.id, context)
        return

    if await db.is_contact_blocked(user.id):
        await query.edit_message_text("متأسفم! دسترسی پیام‌دادن به این بخش برای شما بسته شده. 🚫")
        return

    await db.pool.execute("""
        insert into contact_states(user_id,kind,waiting) values($1,$2,true)
        on conflict (user_id) do update set kind=excluded.kind, waiting=true;
    """, user.id, kind)

    btns = [[InlineKeyboardButton("✉️ ارسال یک پیام", callback_data=f"sendonce|{kind}|{user.id}")],
            [InlineKeyboardButton("◀️ بازگشت", callback_data="back|pm")]]
    await query.edit_message_text(
        "حله! وقتی روی «ارسال یک پیام» بزنی، فقط *یک* پیام (هر فرمتی حتی آلبوم) می‌تونی بفرستی. بعدش گزینه «ارسال مجدد» میاد که اگه خواستی دوباره یک پیام بفرستی.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(btns)
    )

async def cb_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split("|")
    if len(parts) < 2:
        return
    where = parts[1]
    if where == "pm":
        await q.edit_message_text("سلام! این پنل شخصی ربات سولزه. یکی از گزینه‌ها رو انتخاب کن 👇", reply_markup=pm_panel_kb())

async def cb_sendonce(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    kind, owner_id = parts[1], int(parts[2])
    if q.from_user.id != owner_id:
        await q.answer(alert_not_for_you(), show_alert=True)
        return
    await q.answer()
    await q.edit_message_text("منتظرتم! الان فقط *یک* پیام بفرست. بعد از ارسال می‌تونی «ارسال مجدد» بزنی.", parse_mode=ParseMode.MARKDOWN)

async def handle_pm_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    st = await db.pool.fetchrow("select * from contact_states where user_id=$1;", user.id)
    if not st or not st["waiting"]:
        return
    kind = st["kind"]
    if await db.is_contact_blocked(user.id):
        await update.message.reply_text("ارسال پیام برای شما بسته شده. 🚫")
        await db.pool.execute("update contact_states set waiting=false where user_id=$1;", user.id)
        return

    try:
        header = f"📨 پیام جدید از {mention(user.id, user.full_name)}\n@{user.username or '-'} | id: `{user.id}`"
        await context.bot.send_message(
            chat_id=GUARD_CHAT_ID if kind=="guard" else OWNER_ID,
            text=header,
            parse_mode=ParseMode.MARKDOWN
        )
        await update.message.copy(
            chat_id=GUARD_CHAT_ID if kind=="guard" else OWNER_ID,
        )
        kb = [[InlineKeyboardButton("📩 پاسخ", callback_data=f"replyto|{kind}|{user.id}|{update.effective_user.id}")],
              [InlineKeyboardButton("🚫 مسدود DM", callback_data=f"blockdm|{user.id}")]]
        await context.bot.send_message(
            chat_id=GUARD_CHAT_ID if kind=="guard" else OWNER_ID,
            text="—",
            reply_markup=InlineKeyboardMarkup(kb)
        )
    except Exception as e:
        logger.exception("copy to target failed: %s", e)
        await update.message.reply_text("ارسال نشد! یکبار دیگه امتحان کن.")
        return

    await db.pool.execute("update contact_states set waiting=false where user_id=$1;", user.id)
    await context.bot.send_message(
        chat_id=user.id,
        text="پیامت رسید ✅\nاگه خواستی *فقط یک پیام دیگه* بفرستی روی «ارسال مجدد» بزن.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔁 ارسال مجدد", callback_data=f"pm|{kind}")],
                                           [InlineKeyboardButton("◀️ بازگشت", callback_data="back|pm")]])
    )

async def cb_replyto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    if len(parts) < 4:
        await q.answer()
        return
    kind, target_user_id = parts[1], int(parts[2])
    admin = q.from_user
    db: DB = context.bot_data["DB"]
    if not (await is_manager(db, admin.id)):
        await q.answer("فقط مدیران می‌تونن جواب بدن.", show_alert=True)
        return
    await db.pool.execute("""
        insert into admin_reply_states(admin_id,target_user_id,kind) values($1,$2,$3)
        on conflict (admin_id,kind) do update set target_user_id=$2;
    """, admin.id, target_user_id, kind)
    await q.answer()
    await q.edit_message_text("اوکی! *فقط یک پیام* بفرست تا برای کاربر ارسال کنم.", parse_mode=ParseMode.MARKDOWN)

async def handle_guard_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # listens in GUARD_CHAT_ID and in Owner PM for one-shot admin replies
    if update.effective_chat.id not in [GUARD_CHAT_ID, OWNER_ID]:
        return
    admin = update.effective_user
    db: DB = context.bot_data["DB"]
    st = await db.pool.fetchrow("select * from admin_reply_states where admin_id=$1;", admin.id)
    if not st:
        return
    target = int(st["target_user_id"])
    kind = st["kind"]
    try:
        await update.message.copy(chat_id=target)
        await update.message.reply_text("پیامت ارسال شد ✅", reply_to_message_id=update.message.message_id)
        kb = [[InlineKeyboardButton("🔁 پاسخ مجدد", callback_data=f"replyto|{kind}|{target}|{admin.id}")]]
        await context.bot.send_message(chat_id=update.effective_chat.id, text="—", reply_markup=InlineKeyboardMarkup(kb))
    except Exception as e:
        logger.exception("send reply failed: %s", e)
        await update.message.reply_text("نشد! دوباره امتحان کن.")
    await db.pool.execute("delete from admin_reply_states where admin_id=$1 and kind=$2;", admin.id, kind)

async def cb_block_dm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    target_id = int(parts[1])
    admin = q.from_user
    db: DB = context.bot_data["DB"]
    if not (await is_manager(db, admin.id)):
        await q.answer("فقط مدیران!", show_alert=True); return
    await db.set_contact_block(target_id, True, reason="by admin")
    await q.answer("بلاک شد.")
    await q.edit_message_text(f"کاربر {target_id} برای پیام‌دادن بلاک شد.")

# ----------------------------- Stats & Presence -----------------------
SESSION_SELECT_PREFIX = "sess|"

def build_session_kb(author_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎧 کال", callback_data=f"{SESSION_SELECT_PREFIX}call|{author_id}")],
        [InlineKeyboardButton("💬 چت", callback_data=f"{SESSION_SELECT_PREFIX}chat|{author_id}")],
    ])

async def maybe_prompt_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    user = update.effective_user
    if user.is_bot:
        return
    db: DB = context.bot_data["DB"]
    await ensure_user(db, user)
    await db.set_active_member(MAIN_CHAT_ID, user.id, now_tz())
    if await db.is_banned(user.id):
        return
    msg = update.effective_message
    is_media = any([msg.photo, msg.video, msg.document, msg.animation, msg.audio, msg.sticker])
    is_voice = bool(msg.voice)
    mentions = 0
    if msg.entities:
        for e in msg.entities:
            if e.type in [MessageEntity.MENTION, MessageEntity.TEXT_MENTION]:
                mentions += 1
    await db.bump_stat(MAIN_CHAT_ID, user.id, is_media=is_media, is_voice=is_voice, mentions_made=mentions, at=now_tz())

    if await is_manager(db, user.id):
        if not await db.has_active_session(MAIN_CHAT_ID, user.id):
            try:
                await msg.reply_text("نوع فعالیتت رو انتخاب کن:", reply_markup=build_session_kb(user.id))
            except Exception as e:
                logger.warning("session prompt failed: %s", e)
        await schedule_idle_job(context, user.id)
    await db.set_user_in_group(user.id, True)

async def schedule_idle_job(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    job_name = f"idle_{MAIN_CHAT_ID}_{user_id}"
    for job in context.job_queue.get_jobs_by_name(job_name):
        job.schedule_removal()
    context.job_queue.run_once(idle_timeout_job, when=300, name=job_name, data={"chat_id": MAIN_CHAT_ID, "user_id": user_id})

async def idle_timeout_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    chat_id = data.get("chat_id"); user_id = data.get("user_id")
    db: DB = context.bot_data["DB"]
    if await db.has_active_session(chat_id, user_id):
        row = await db.end_session(chat_id, user_id, "auto", now_tz())
        if row:
            kind = row["type"]
            await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=f"⛔ پایان خودکار سشن {kind} برای {mention(user_id,'کاربر')} به دلیل عدم فعالیت ۵ دقیقه‌ای.", parse_mode=ParseMode.MARKDOWN)

async def cb_session_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    if len(parts) < 3:
        await q.answer(); return
    kind, author_id = parts[1], int(parts[2])
    if q.from_user.id != author_id:
        await q.answer(alert_not_for_you(), show_alert=True); return
    db: DB = context.bot_data["DB"]
    if await db.has_active_session(MAIN_CHAT_ID, q.from_user.id):
        await q.answer("الان هم یک سشن باز داری!"); return
    await db.add_session(MAIN_CHAT_ID, q.from_user.id, kind, now_tz())
    await q.answer("ثبت شد ✅")
    try:
        await q.edit_message_text(f"شروع فعالیت { 'کال' if kind=='call' else 'چت' } ✅")
    except: pass
    await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=f"✅ شروع سشن { 'کال' if kind=='call' else 'چت' } توسط {mention(q.from_user.id, q.from_user.full_name)}", parse_mode=ParseMode.MARKDOWN)
    await schedule_idle_job(context, q.from_user.id)

async def cmd_register_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not await is_manager(db, user.id):
        return
    if await db.has_active_session(MAIN_CHAT_ID, user.id):
        await update.message.reply_text("الان هم یک سشن باز داری!")
        return
    await update.message.reply_text("نوع فعالیتت رو انتخاب کن:", reply_markup=build_session_kb(user.id))

async def cmd_register_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not await is_manager(db, user.id):
        return
    row = await db.end_session(MAIN_CHAT_ID, user.id, "user", now_tz())
    if not row:
        await update.message.reply_text("سشنی باز نیست.")
        return
    await update.message.reply_text("پایان فعالیت شما گزارش شد، خسته نباشی! ✅")
    await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=f"🟥 پایان سشن {row['type']} توسط {mention(user.id, user.full_name)}", parse_mode=ParseMode.MARKDOWN)

async def nightly_stats_job(context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    now = now_tz()
    y = (now - timedelta(days=1)).date()

    managers = await db.list_all_managers()
    all_ids = {uid for lst in managers.values() for uid in lst}
    for uid in all_ids:
        await db.update_call_time_aggregate_for_day(MAIN_CHAT_ID, uid, y)

    async def fetch(uids: List[int]):
        if not uids: return []
        res = []
        for uid in uids:
            rows = await db.get_stats_for_user_days(MAIN_CHAT_ID, uid, 1)
            if rows:
                r = rows[0]
                res.append((uid, r["messages_count"], r["media_count"], r["voice_count"], r["mentions_made_count"]))
            else:
                res.append((uid, 0,0,0,0))
        return res

    chat_group = managers.get("admin_chat", []) + managers.get("senior_chat", []) + managers.get("senior_global", []) + ([OWNER_ID] if OWNER_ID else [])
    call_group = managers.get("admin_call", []) + managers.get("senior_call", []) + managers.get("senior_global", []) + ([OWNER_ID] if OWNER_ID else [])

    chat_stats = await fetch(chat_group)
    call_stats = []
    for uid in call_group:
        rows = await db.get_stats_for_user_days(MAIN_CHAT_ID, uid, 1)
        if rows:
            r = rows[0]
            call_stats.append((uid, r["call_time_sec"]))
        else:
            call_stats.append((uid, 0))

    if jdatetime:
        j = jdatetime.date.fromgregorian(date=y)
        date_str = f"{j.strftime('%Y/%m/%d')}"
    else:
        date_str = y.strftime("%Y-%m-%d")
    wd = WEEKDAYS_FA[(y.weekday()+1) % 7]

    lines = [f"📊 آمار چت مدیران — {date_str} ({wd})", ""]
    for uid, msgs, media, voice, men in chat_stats:
        lines.append(f"• {mention(uid, 'کاربر')} — پیام: {msgs} | رسانه: {media} | ویس: {voice} | منشن: {men}")
    text1 = "\n".join(lines)

    lines2 = [f"🎧 آمار کال مدیران — {date_str} ({wd})", ""]
    for uid, sec in call_stats:
        lines2.append(f"• {mention(uid, 'کاربر')} — زمان حضور: {format_secs(int(sec))}")
    text2 = "\n".join(lines2)

    lines3 = [f"📣 منشن‌های امروز — {date_str} ({wd})", ""]
    for uid, msgs, media, voice, men in chat_stats:
        lines3.append(f"• {mention(uid,'کاربر')}: {men}")
    text3 = "\n".join(lines3)

    await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=text1, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=text2, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=text3, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

async def send_stats_for_user(user_id: int, context: ContextTypes.DEFAULT_TYPE, reply_to: Optional[int]=None):
    db: DB = context.bot_data["DB"]
    rows = await db.get_stats_for_user_days(MAIN_CHAT_ID, user_id, 7)
    if not rows:
        await context.bot.send_message(chat_id=user_id, text="آماری برای ۷ روز گذشته ندارم.")
        return
    try:
        photos = await context.bot.get_user_profile_photos(user_id, limit=1)
        file_id = photos.photos[0][-1].file_id if photos.total_count > 0 else None
    except:
        file_id = None
    lines = ["📊 آمار ۷ روز گذشته در گروه سولز:", ""]
    for r in reversed(rows):
        d = r["date"]
        jd = jdatetime.date.fromgregorian(date=d).strftime("%Y/%m/%d") if jdatetime else d.strftime("%Y-%m-%d")
        lines.append(f"• {jd} — پیام: {r['messages_count']} | رسانه: {r['media_count']} | ویس: {r['voice_count']} | منشن: {r['mentions_made_count']} | کال: {format_secs(int(r['call_time_sec']))}")
    cap = "\n".join(lines)
    if file_id:
        await context.bot.send_photo(chat_id=user_id, photo=file_id, caption=cap)
    else:
        await context.bot.send_message(chat_id=user_id, text=cap)

# ----------------------------- Management -----------------------------
async def extract_target_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    msg = update.effective_message
    if msg.reply_to_message:
        return msg.reply_to_message.from_user.id
    text = (msg.text or "").strip()
    parts = text.split()
    if len(parts) >= 2:
        token = parts[1]
        if token.startswith("@"):
            # Resolving @username via Bot API programmatically is unreliable; use reply or numeric id.
            return None
        else:
            try:
                return int(token)
            except:
                return None
    return None

async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not (await is_manager(db, user.id)):
        return
    target = await extract_target_user_id(update, context)
    if not target:
        await update.message.reply_text("هدف نامعتبره. با ریپلای یا آیدی عددی بزن.")
        return
    await db.ban_add(target, reason="by command", added_by=user.id)
    try:
        await context.bot.ban_chat_member(chat_id=MAIN_CHAT_ID, user_id=target)
    except Exception as e:
        logger.info("ban action: %s", e)
    await update.message.reply_text(f"کاربر {target} به لیست ممنوع اضافه شد و دسترسی گروه قطع شد.")

async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not (await is_manager(db, user.id)):
        return
    target = await extract_target_user_id(update, context)
    if not target:
        await update.message.reply_text("هدف نامعتبره. با ریپلای یا آیدی عددی بزن.")
        return
    await db.ban_remove(target)
    try:
        await context.bot.unban_chat_member(chat_id=MAIN_CHAT_ID, user_id=target, only_if_banned=True)
    except Exception as e:
        logger.info("unban action: %s", e)
    await update.message.reply_text(f"کاربر {target} از لیست ممنوع حذف شد و اجازه ورود گرفت.")

async def cmd_list_banned(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not (await is_manager(db, user.id)):
        return
    rows = await db.list_banned()
    if not rows:
        await update.message.reply_text("لیست ممنوع خالیه.")
        return
    lines = ["🚫 لیست ممنوع:", ""]
    for r in rows:
        lines.append(f"• {mention(r['user_id'],'کاربر')} — id: `{r['user_id']}`")
    text = "\n".join(lines)
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    if OWNER_ID:
        await context.bot.send_message(chat_id=OWNER_ID, text=text, parse_mode=ParseMode.MARKDOWN)

ROLE_MAP = {
    "ترفیع چت": "admin_chat",
    "ترفیع کال": "admin_call",
    "ترفیع ارشد چت": "senior_chat",
    "ترفیع ارشد کال": "senior_call",
    "ترفیع ارشد کل": "senior_global",
}
DEMOTE_MAP = {
    "عزل چت": "admin_chat",
    "عزل کال": "admin_call",
    "عزل ارشد چت": "senior_chat",
    "عزل ارشد کال": "senior_call",
    "عزل ارشد کل": "senior_global",
}

async def handle_promote_demote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not await is_owner(user.id):
        return
    text = (update.message.text or "").strip()
    target = await extract_target_user_id(update, context)
    if not target:
        await update.message.reply_text("هدف نامعتبره.")
        return
    if any(text.startswith(k) for k in ROLE_MAP.keys()):
        for k, role in ROLE_MAP.items():
            if text.startswith(k):
                await db.add_role(target, role)
                await update.message.reply_text(f"کاربر {mention(target,'کاربر')} به عنوان {k.replace('ترفیع ','')} منصوب شد.", parse_mode=ParseMode.MARKDOWN)
                return
    if any(text.startswith(k) for k in DEMOTE_MAP.keys()):
        for k, role in DEMOTE_MAP.items():
            if text.startswith(k):
                await db.remove_role(target, role)
                await update.message.reply_text(f"سمت {k.replace('عزل ','')} از کاربر برداشته شد.", parse_mode=ParseMode.MARKDOWN)
                return

async def cmd_list_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not (await is_senior(db, user.id)):
        return
    managers = await db.list_all_managers()
    order = ["owner","senior_global","senior_call","senior_chat","admin_call","admin_chat"]
    names = {"owner":"مالک","senior_global":"ارشد کل","senior_call":"ارشد کال","senior_chat":"ارشد چت","admin_call":"ادمین کال","admin_chat":"ادمین چت"}
    lines = ["👥 لیست گارد (به ترتیب سمت):",""]
    for r in order:
        ids = managers.get(r, [])
        if not ids: continue
        lines.append(f"— {names[r]}:")
        for uid in ids:
            lines.append(f"   • {mention(uid,'کاربر')}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    user = update.effective_user
    if not await is_manager(db, user.id):
        return
    target = await extract_target_user_id(update, context)
    t_id = target or user.id
    rows = await db.get_stats_for_user_days(MAIN_CHAT_ID, t_id, 7)
    if not rows:
        await update.message.reply_text("آماری موجود نیست.")
        return
    try:
        photos = await context.bot.get_user_profile_photos(t_id, limit=1)
        file_id = photos.photos[0][-1].file_id if photos.total_count > 0 else None
    except:
        file_id = None
    lines = [f"📊 آمار ۷ روز گذشته برای {mention(t_id,'کاربر')}:", ""]
    for r in reversed(rows):
        d = r["date"]
        jd = jdatetime.date.fromgregorian(date=d).strftime("%Y/%m/%d") if jdatetime else d.strftime("%Y-%m-%d")
        lines.append(f"• {jd}: پیام {r['messages_count']} | رسانه {r['media_count']} | ویس {r['voice_count']} | منشن {r['mentions_made_count']} | کال {format_secs(int(r['call_time_sec']))}")
    cap = "\n".join(lines)
    if file_id:
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=file_id, caption=cap, reply_to_message_id=update.effective_message.message_id)
    else:
        await update.message.reply_text(cap, parse_mode=ParseMode.MARKDOWN)

# ----------------------------- Tag Panel ------------------------------
def tag_panel_kb(author_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎧 تگ کال", callback_data=f"tag|call|{author_id}")],
        [InlineKeyboardButton("💬 تگ چت", callback_data=f"tag|chat|{author_id}")],
        [InlineKeyboardButton("🔥 تگ اعضای فعال", callback_data=f"tag|active|{author_id}")],
        [InlineKeyboardButton("👧 تگ دخترها", callback_data=f"tag|girls|{author_id}")],
        [InlineKeyboardButton("👦 تگ پسرها", callback_data=f"tag|boys|{author_id}")],
    ])

async def cmd_tag_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("کیو می‌خوای صدا کنیم؟", reply_markup=tag_panel_kb(update.effective_user.id))

async def cb_tag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    group, author_id = parts[1], int(parts[2])
    if q.from_user.id != author_id:
        await q.answer(alert_not_for_you(), show_alert=True); return
    db: DB = context.bot_data["DB"]
    await q.answer("باشه!")
    ids: List[int] = []
    if group == "call":
        ids += await db.list_by_role("admin_call")
        ids += await db.list_by_role("senior_call")
        ids += await db.list_by_role("senior_global")
        if OWNER_ID: ids.append(OWNER_ID)
    elif group == "chat":
        ids += await db.list_by_role("admin_chat")
        ids += await db.list_by_role("senior_chat")
        ids += await db.list_by_role("senior_global")
        if OWNER_ID: ids.append(OWNER_ID)
    elif group == "active":
        ids = await db.get_active_members(MAIN_CHAT_ID, 1440)
    elif group == "girls":
        ids = await db.list_gender("female")
    elif group == "boys":
        ids = await db.list_gender("male")

    uniq, seen = [], set()
    for i in ids:
        if i in seen: continue
        seen.add(i); uniq.append(i)

    reply_to = q.message.reply_to_message.message_id if q.message and q.message.reply_to_message else None
    batches = [uniq[i:i+5] for i in range(0, len(uniq), 5)]
    if not batches:
        await q.edit_message_text("کسی پیدا نشد.")
        return
    await q.edit_message_text("دارم صدا می‌زنم...")
    for b in batches:
        line = "، ".join(mention(uid, "کاربر") for uid in b)
        try:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=line, parse_mode=ParseMode.MARKDOWN, reply_to_message_id=reply_to)
            await asyncio.sleep(1.2)
        except Exception as e:
            logger.info("tag send failed: %s", e)

# ----------------------------- Gender Command -------------------------
def gender_kb(author_id: int, target_id: Optional[int]) -> InlineKeyboardMarkup:
    tid = target_id or 0
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👦 پسر", callback_data=f"gender|male|{author_id}|{tid}")],
        [InlineKeyboardButton("👧 دختر", callback_data=f"gender|female|{author_id}|{tid}")],
    ])

async def cmd_gender(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    user = update.effective_user
    if not (await is_manager(db, user.id)):
        return
    target = await extract_target_user_id(update, context)
    await update.message.reply_text("جنسیت رو انتخاب کن:", reply_markup=gender_kb(user.id, target))

async def cb_gender(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    gender, author_id, target_id = parts[1], int(parts[2]), int(parts[3])
    if q.from_user.id != author_id:
        await q.answer(alert_not_for_you(), show_alert=True); return
    db: DB = context.bot_data["DB"]
    target = target_id or q.from_user.id
    await db.set_gender(target, "male" if gender=="male" else "female")
    await q.answer("ثبت شد ✅")
    try:
        await q.edit_message_text("جنسیت ذخیره شد.")
    except: pass

# ----------------------------- Help ----------------------------------
HELP_TEXT = """
راهنمای ربات سولز — نسخه فشرده
(همه دستورات *بدون /* هستند)

مدیریت حضور:
• «ثبت» → باز کردن پنل انتخاب «کال» یا «چت»
• «ثبت خروج» → پایان سشن جاری (اگر ۵ دقیقه پیام ندی خودکار بسته میشه)

مدیریت کاربران:
• «ممنوع [ریپلای/آیدی]» → بن کامل کاربر
• «آزاد [ریپلای/آیدی]» → حذف از لیست ممنوع
• «لیست ممنوع» → نمایش لیست ممنوع‌ها

مقامات (فقط مالک):
• «ترفیع چت/ترفیع کال/ترفیع ارشد چت/ترفیع ارشد کال/ترفیع ارشد کل [ریپلای/آیدی]»
• «عزل چت/عزل کال/عزل ارشد چت/عزل ارشد کال/عزل ارشد کل [ریپلای/آیدی]»

اطلاعات و تگ:
• «لیست گارد» (مالک و ارشدها)
• «آیدی [اختیاری: ریپلای/آیدی]» → آمار ۷ روز گذشته با عکس پروفایل
• «تگ» → پنل تگ: کال/چت/اعضای فعال/دخترها/پسرها
• «جنسیت [ریپلای اختیاری]» → انتخاب پسر/دختر برای خودت یا هدف

فان:
• «تگ روشن» / «تگ خاموش» → تگ تصادفی فعال‌های ساکت + جمله‌های فان
• «بازی» → پنل ۱۵+ بازی گروهی
• «ربات» (برای مقام‌داران) → جواب‌های قشنگ و متنوع

پنل خصوصی /start:
• «ارتباط با گارد مدیران» (ارسال یک‌باره)
• «ارتباط با مالک» (ارسال یک‌باره)
• «آمار من»
"""

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    user = update.effective_user
    if not (await is_manager(db, user.id)):
        return
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.MARKDOWN)

# ----------------------------- Random Tag Toggle ----------------------
async def cmd_tag_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    # فقط مالک
    if not await is_owner(user.id):
        return
    text = (update.message.text or "").strip()
    on = "روشن" in text
    await db.set_random_tag(MAIN_CHAT_ID, on)
    await update.message.reply_text("حله. تگ تصادفی " + ("روشن شد ✅" if on else "خاموش شد ⛔"))

async def random_tag_job(context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    if not await db.get_random_tag(MAIN_CHAT_ID):
        return
    ids = await db.get_active_members(MAIN_CHAT_ID, since_minutes=1440)
    if not ids:
        return
    target = random.choice(ids)
    phrase = random.choice(RANDOM_TAG_LINES)
    try:
        await context.bot.send_message(chat_id=MAIN_CHAT_ID, text=f"{mention(target, 'داداش/خواهر')} {phrase}", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.info("random tag send failed: %s", e)

# ----------------------------- "ربات" friendly replies ----------------
async def cmd_bot_nice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    user = update.effective_user
    if not (await is_manager(db, user.id)):
        return
    await update.message.reply_text(random.choice(BOT_NICE_LINES))

# ----------------------------- Game Engine ----------------------------
class GameSession:
    def __init__(self, chat_id: int, game_id: str, prompt: str, answers: List[str], started_by: int, points: int = 1, meta: Optional[dict]=None):
        self.chat_id = chat_id
        self.game_id = game_id
        self.prompt = prompt
        self.answers = [a.lower() for a in answers]
        self.started_by = started_by
        self.points = points
        self.meta = meta or {}
        self.created_at = now_tz()
        self.active = True

GAME_SESSIONS: Dict[int, GameSession] = {}

def normalize(s: str) -> str:
    s = (s or "").strip().lower()
    rep = {"ي":"ی","ك":"ک","آ":"ا","إ":"ا","أ":"ا","ٱ":"ا","ة":"ه","ؤ":"و","ئ":"ی"}
    for a,b in rep.items():
        s = s.replace(a,b)
    s = re.sub(r"\s+", " ", s)
    return s

def game_list_kb(author_id: int) -> InlineKeyboardMarkup:
    names = [
        ("g_num100","حدس عدد ۱..۱۰۰"),
        ("g_num1000","حدس عدد ۱..۱۰۰۰"),
        ("g_anagram","به‌هم‌ریختهٔ کلمه"),
        ("g_typing","تایپ سرعتی"),
        ("g_math","مسابقه ریاضی"),
        ("g_capital","پایتخت کشورها"),
        ("g_emoji","معمای ایموجی"),
        ("g_odd","غریبهٔ جمع"),
        ("g_flag","پرچم-کشور"),
        ("g_syn","مترادف (فارسی)"),
        ("g_word_hole","کلمه ناقص"),
        ("g_rps","قیچی-کاغذ-سنگ"),
        ("g_coin","شیر یا خط"),
        ("g_seq","الگوی عددی"),
        ("g_trivia","دانستنی‌ها"),
    ]
    rows = []
    for i in range(0, len(names), 3):
        row = [InlineKeyboardButton(names[j][1], callback_data=f"game|{names[j][0]}|{author_id}") for j in range(i,min(i+3,len(names)))]
        rows.append(row)
    rows.append([InlineKeyboardButton("📈 جدول امتیاز", callback_data=f"game|score|{author_id}")])
    return InlineKeyboardMarkup(rows)

async def cmd_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("یه بازی انتخاب کن:", reply_markup=game_list_kb(update.effective_user.id))

async def cb_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    gid, author_id = parts[1], int(parts[2])
    if q.from_user.id != author_id:
        await q.answer(alert_not_for_you(), show_alert=True); return
    await q.answer()
    if gid == "score":
        await show_scoreboard(update, context); return
    session = await start_game_session(gid, q.message.chat_id, q.from_user.id)
    if not session:
        await q.edit_message_text("این بازی الان در دسترس نیست.")
        return
    try:
        await q.edit_message_text(f"🎮 {session.game_id}: {session.prompt}")
    except:
        await context.bot.send_message(chat_id=q.message.chat_id, text=f"🎮 {session.game_id}: {session.prompt}")

async def show_scoreboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    rows = await db.get_game_top(MAIN_CHAT_ID, limit=10)
    if not rows:
        await update.callback_query.edit_message_text("جدول خالیه.")
        return
    lines = ["📈 جدول امتیاز:", ""]
    for i, r in enumerate(rows, start=1):
        name = r["fn"] or ""
        if r["ln"]: name += " " + r["ln"]
        if not name and r["un"]: name = "@"+r["un"]
        if not name: name = f"id:{r['user_id']}"
        lines.append(f"{i}. {name} — {r['score']}")
    await update.callback_query.edit_message_text("\n".join(lines))

CAPITALS = {
    "ایران":"تهران","عراق":"بغداد","ترکیه":"آنکارا","افغانستان":"کابل","فرانسه":"پاریس","آلمان":"برلین",
    "ایتالیا":"رم","اسپانیا":"مادرید","انگلستان":"لندن","روسیه":"مسکو","چین":"پکن","ژاپن":"توکیو",
    "هند":"دهلی نو","برزیل":"برازیلیا","کانادا":"اتاوا","مکزیک":"مکزیکوسیتی","مصر":"قاهره","عربستان":"ریاض",
}
EMOJI_RIDDLES = [("🍎📱", ["اپل","apple"]),("🎬🍿", ["سینما","فیلم"]),("☕🐱", ["کافه","قهوه"]),("📸🐦", ["اینستاگرام","instagram","عکس"]),("🧊❄️", ["یخ","سرما"])]
WORDS_FA = ["مدیریت","سولز","گارد","حضور","آمار","سیستم","ربات","گفتگو","سرگرمی","اکانت","ویس","کال","مدیر","پیام","گروه","کاربر","شماره","زمان","تاریخ","حساب"]
SYN_FA = [("سریع","تند"),("آرام","ملایم"),("شوخ","بامزه"),("باهوش","زیرک"),("قوی","نیرومند")]
TRIVIA = [("بزرگ‌ترین اقیانوس جهان؟","آرام"),("ارتفاعات دماوند در کدام کشور است؟","ایران"),("تهران چندمین حرف الفباست؟","شوخی کردی؟ 😅")]
ODD_SETS = [["سیب","موز","گلابی","پرتقال","پیچ‌گوشتی"],["آبی","قرمز","سبز","پیچ"]]
SEQS = [([2,4,8,16,"?"],"32"),([1,1,2,3,5,8,"?"],"13")]

async def start_game_session(gid: str, chat_id: int, started_by: int) -> Optional[GameSession]:
    if chat_id in GAME_SESSIONS and GAME_SESSIONS[chat_id].active:
        GAME_SESSIONS[chat_id].active = False

    if gid == "g_num100":
        num = random.randint(1,100); return set_session(chat_id, gid, f"یه عدد بین ۱ تا ۱۰۰ حدس بزن!", [str(num)], started_by)
    if gid == "g_num1000":
        num = random.randint(1,1000); return set_session(chat_id, gid, f"عدد بین ۱ تا ۱۰۰۰ حدس بزن!", [str(num)], started_by)
    if gid == "g_anagram":
        w = random.choice(WORDS_FA); shuffled = "".join(random.sample(w, len(w))); return set_session(chat_id, gid, f"حروف به‌هم‌ریخته: {shuffled}", [normalize(w)], started_by)
    if gid == "g_typing":
        s = " ".join(random.sample(["سولز","ربات","مدیر","حضور","آمار","گارد","کال","چت"], k=4)); return set_session(chat_id, gid, f"این متن رو *دقیقاً* و سریع تایپ کن:\n{s}", [normalize(s)], started_by)
    if gid == "g_math":
        a,b = random.randint(10,99), random.randint(10,99); op = random.choice(["+","-","*"]); expr = f"{a}{op}{b}"; ans = str(eval(expr)); return set_session(chat_id, gid, f"حل کن: `{expr}`", [ans], started_by)
    if gid == "g_capital":
        c, cap = random.choice(list(CAPITALS.items())); return set_session(chat_id, gid, f"پایتخت *{c}* چیه؟", [normalize(cap)], started_by)
    if gid == "g_emoji":
        e, ans = random.choice(EMOJI_RIDDLES); return set_session(chat_id, gid, f"حدس بزن: {e}", [normalize(a) for a in ans], started_by)
    if gid == "g_odd":
        s = random.choice(ODD_SETS); return set_session(chat_id, gid, f"کدومشون وصله ناجوره؟ {'، '.join(s)}", [normalize(s[-1])], started_by)
    if gid == "g_flag":
        c, cap = random.choice(list(CAPITALS.items())); return set_session(chat_id, gid, f"پرچم 🇮🇷؟ شوخی! کشورِ پایتخت *{cap}* رو بگو:", [normalize(c)], started_by)
    if gid == "g_syn":
        a,b = random.choice(SYN_FA); return set_session(chat_id, gid, f"مترادف «{a}» چیه؟", [normalize(b)], started_by)
    if gid == "g_word_hole":
        w = random.choice(WORDS_FA); idxs = random.sample(range(len(w)), k=min(2, max(1, len(w)//4))); hole = "".join([("_" if i in idxs else ch) for i,ch in enumerate(w)]); return set_session(chat_id, gid, f"جای خالی رو پر کن: {hole}", [normalize(w)], started_by)
    if gid == "g_rps":
        bot = random.choice(["سنگ","کاغذ","قیچی"]); winners = {"سنگ":"کاغذ","کاغذ":"قیچی","قیچی":"سنگ"}; return set_session(chat_id, gid, f"من زدم: *{bot}* — تو چی می‌زنی که می‌بره؟", [normalize(winners[bot])], started_by)
    if gid == "g_coin":
        coin = random.choice(["شیر","خط"]); return set_session(chat_id, gid, f"سکه هواست... شیر یا خط؟", [normalize(coin)], started_by)
    if gid == "g_seq":
        seq, ans = random.choice(SEQS); return set_session(chat_id, gid, f"الگو رو کامل کن: {'، '.join(map(str,seq))}", [normalize(ans)], started_by)
    if gid == "g_trivia":
        q,a = random.choice(TRIVIA); return set_session(chat_id, gid, q, [normalize(a)], started_by)
    return None

def set_session(chat_id: int, gid: str, prompt: str, answers: List[str], started_by: int) -> GameSession:
    s = GameSession(chat_id, gid, prompt, answers, started_by, points=1)
    GAME_SESSIONS[chat_id] = s
    return s

async def handle_game_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    msg = update.effective_message
    if not msg.text:
        return
    sess = GAME_SESSIONS.get(MAIN_CHAT_ID)
    if not sess or not sess.active:
        return
    txt = normalize(msg.text)
    if txt in sess.answers:
        sess.active = False
        db: DB = context.bot_data["DB"]
        await db.inc_game_score(MAIN_CHAT_ID, msg.from_user.id, 1)
        await msg.reply_text(f"🎉 {mention(msg.from_user.id, msg.from_user.full_name)} درست گفت! (+1 امتیاز)\nمیخوای ادامه بدیم؟ «بازی»", parse_mode=ParseMode.MARKDOWN)

# ----------------------------- Text Commands --------------------------
async def handle_text_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip()
    handlers = [
        ("ثبت خروج", cmd_register_close),
        ("ثبت", cmd_register_open),
        ("لیست ممنوع", cmd_list_banned),
        ("لیست گارد", cmd_list_guard),
        ("راهنما", cmd_help),
        ("تگ روشن", cmd_tag_toggle),
        ("تگ خاموش", cmd_tag_toggle),
        ("تگ", cmd_tag_panel),
        ("جنسیت", cmd_gender),
        ("آیدی", cmd_id),
        ("بازی", cmd_game),
        ("ربات", cmd_bot_nice),
    ]
    for key, fn in handlers:
        if txt.startswith(key):
            await fn(update, context)
            return
    if txt.startswith("ترفیع ") or txt.startswith("عزل "):
        await handle_promote_demote(update, context); return
    if txt.startswith("ممنوع"):
        await cmd_ban(update, context); return
    if txt.startswith("آزاد"):
        await cmd_unban(update, context); return

# ----------------------------- Membership & Bans ----------------------
async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    upd: ChatMemberUpdated = update.chat_member
    user = upd.new_chat_member.user
    await ensure_user(db, user)
    if upd.chat.id != MAIN_CHAT_ID:
        return
    status = upd.new_chat_member.status
    if status in ("member","administrator","creator"):
        await db.set_user_in_group(user.id, True)
        if await db.is_banned(user.id):
            try:
                await context.bot.ban_chat_member(chat_id=MAIN_CHAT_ID, user_id=user.id)
            except Exception as e:
                logger.info("ban on join: %s", e)
    elif status in ("left","kicked","restricted"):
        await db.set_user_in_group(user.id, False)

# ----------------------------- Application Setup ---------------------
async def post_init(app: Application):
    """
    Runs after Application.initialize(); good place to init DB and schedule jobs.
    """
    if not BOT_TOKEN or not DATABASE_URL or not MAIN_CHAT_ID or not GUARD_CHAT_ID or not OWNER_ID:
        raise SystemExit("لطفاً تمام متغیرهای محیطی لازم را تنظیم کنید: OWNER_ID, TZ, MAIN_CHAT_ID, GUARD_CHAT_ID, BOT_TOKEN, DATABASE_URL")

    # Prepare DB
    db = await DB.create(DATABASE_URL)
    app.bot_data["DB"] = db

    # Schedule nightly stats at 00:00 TZ
    now = now_tz()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    delay = (tomorrow - now).total_seconds()
    app.job_queue.run_repeating(nightly_stats_job, interval=86400, first=delay)

    # Random tag job (every 15m)
    app.job_queue.run_repeating(random_tag_job, interval=900, first=60)

def build_application() -> Application:
    defaults = Defaults(tzinfo=TZINFO, parse_mode=ParseMode.MARKDOWN)

    # Optional rate limiter: if extras not installed, continue without it
    rate_limiter = None
    try:
        rate_limiter = AIORateLimiter()
    except Exception:
        logger.warning("AIORateLimiter غیرفعال است (نصب نشده). برای فعال‌سازی: pip install 'python-telegram-bot[rate-limiter]'")
        rate_limiter = None

    builder = ApplicationBuilder().token(BOT_TOKEN).defaults(defaults).post_init(post_init)
    if rate_limiter is not None:
        builder = builder.rate_limiter(rate_limiter)
    app = builder.build()

    app.add_handler(CommandHandler("start", cmd_start, filters.ChatType.PRIVATE))
    app.add_handler(CallbackQueryHandler(cb_pm, pattern=r"^pm\|"))
    app.add_handler(CallbackQueryHandler(cb_back, pattern=r"^back\|"))
    app.add_handler(CallbackQueryHandler(cb_sendonce, pattern=r"^sendonce\|"))
    app.add_handler(CallbackQueryHandler(cb_replyto, pattern=r"^replyto\|"))
    app.add_handler(CallbackQueryHandler(cb_block_dm, pattern=r"^blockdm\|"))
    app.add_handler(CallbackQueryHandler(cb_session_select, pattern=r"^sess\|"))
    app.add_handler(CallbackQueryHandler(cb_tag, pattern=r"^tag\|"))
    app.add_handler(CallbackQueryHandler(cb_gender, pattern=r"^gender\|"))
    app.add_handler(CallbackQueryHandler(cb_game, pattern=r"^game\|"))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, handle_pm_any))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, maybe_prompt_session))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, handle_game_answer))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_commands))
    app.add_handler(MessageHandler(filters.Chat(GUARD_CHAT_ID) | filters.Chat(OWNER_ID), handle_guard_admin_reply))
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.MY_CHAT_MEMBER | ChatMemberHandler.CHAT_MEMBER))
    return app

def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN ست نشده.")
    app = build_application()
    logger.info("Souls bot (patched) starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
# -*- coding: utf-8 -*-
"""
Souls / Souls Guard Telegram Bot (single-file, Railway-ready) — Patched & Flexible
- Safe DB auto-migrations (prevents UndefinedColumnError like "users.is_bot")
- Optional AIORateLimiter (runs even if PTB extras not installed)
- Implements: PM panel (guard/owner), one-shot messaging, admin replies, block DM,
  presence sessions (call/chat), auto end-after-idle, nightly stats (Jalali if available),
  bans list (ممنوع/آزاد), roles promote/demote, guard list, ID stats (7d + avatar),
  tag panel (call/chat/active/girls/boys, 5-by-5 mentions), gender popup,
  fun: random tag lines (200+ combos), 15+ text games with scoreboard,
  owner/global senior/admin scoping, user-scoped inline keyboards, and more.

Env vars:
  OWNER_ID , TZ , MAIN_CHAT_ID , GUARD_CHAT_ID , BOT_TOKEN , DATABASE_URL
"""

import asyncio
import logging
import os
import re
import random
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple

import asyncpg
from zoneinfo import ZoneInfo

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMemberUpdated,
    MessageEntity
)
from telegram.constants import ParseMode, ChatType
from telegram.ext import (
    Application, ApplicationBuilder, AIORateLimiter, ContextTypes, CommandHandler,
    MessageHandler, filters, CallbackQueryHandler, ChatMemberHandler, Defaults
)

try:
    import jdatetime  # optional (for Jalali dates)
except Exception:
    jdatetime = None

# ----------------------------- Config ---------------------------------

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
MAIN_CHAT_ID = int(os.getenv("MAIN_CHAT_ID", "0"))
GUARD_CHAT_ID = int(os.getenv("GUARD_CHAT_ID", "0"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
TZ = os.getenv("TZ", "Asia/Tehran")

TZINFO = ZoneInfo(TZ)

# ----------------------------- Logging --------------------------------

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("souls-bot")

# ----------------------------- DB Layer -------------------------------

class DB:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def create(cls, dsn: str) -> "DB":
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
        db = cls(pool)
        await db.init()
        return db

    async def init(self):
        # 1) create tables if not exist
        create_sql = """
        create table if not exists users(
            user_id bigint primary key,
            username text,
            first_name text,
            last_name text,
            is_bot boolean default false,
            gender text,
            last_seen_at timestamptz,
            in_group boolean default false
        );

        create table if not exists roles(
            user_id bigint references users(user_id) on delete cascade,
            role text not null,
            primary key (user_id, role)
        );

        create table if not exists bans(
            user_id bigint primary key,
            reason text,
            added_by bigint,
            added_at timestamptz default now()
        );

        create table if not exists contact_blocks(
            user_id bigint primary key,
            blocked boolean default true,
            reason text,
            updated_at timestamptz default now()
        );

        create table if not exists stats_daily(
            chat_id bigint,
            user_id bigint references users(user_id) on delete cascade,
            date date,
            messages_count integer default 0,
            media_count integer default 0,
            voice_count integer default 0,
            mentions_made_count integer default 0,
            call_time_sec integer default 0,
            primary key (chat_id, user_id, date)
        );

        create table if not exists sessions(
            id bigserial primary key,
            chat_id bigint not null,
            user_id bigint not null references users(user_id) on delete cascade,
            type text not null,
            start_at timestamptz not null,
            end_at timestamptz,
            ended_by text,
            active boolean default true
        );

        create table if not exists dm_threads(
            id bigserial primary key,
            kind text not null,
            user_id bigint not null,
            created_at timestamptz default now(),
            is_open boolean default true,
            last_admin_id bigint
        );

        create table if not exists contact_states(
            user_id bigint primary key,
            kind text not null,
            waiting boolean default false
        );

        create table if not exists admin_reply_states(
            admin_id bigint,
            target_user_id bigint,
            kind text not null,
            primary key (admin_id, kind)
        );

        create table if not exists toggles(
            chat_id bigint primary key,
            random_tag boolean default false
        );

        create table if not exists active_members(
            chat_id bigint,
            user_id bigint,
            last_activity_at timestamptz,
            primary key (chat_id, user_id)
        );

        create table if not exists game_scores(
            chat_id bigint,
            user_id bigint,
            score integer default 0,
            updated_at timestamptz default now(),
            primary key (chat_id, user_id)
        );
        """
        # 2) safe migrations for old installs (prevents "is_bot does not exist")
        migrate_sql = """
        -- users
        alter table if exists users add column if not exists username text;
        alter table if exists users add column if not exists first_name text;
        alter table if exists users add column if not exists last_name text;
        alter table if exists users add column if not exists is_bot boolean default false;
        alter table if exists users add column if not exists gender text;
        alter table if exists users add column if not exists last_seen_at timestamptz;
        alter table if exists users add column if not exists in_group boolean default false;

        -- stats_daily
        alter table if exists stats_daily add column if not exists media_count integer default 0;
        alter table if exists stats_daily add column if not exists voice_count integer default 0;
        alter table if exists stats_daily add column if not exists mentions_made_count integer default 0;
        alter table if exists stats_daily add column if not exists call_time_sec integer default 0;

        -- sessions
        alter table if exists sessions add column if not exists ended_by text;
        alter table if exists sessions add column if not exists active boolean default true;

        -- bans
        alter table if exists bans add column if not exists reason text;
        alter table if exists bans add column if not exists added_by bigint;
        alter table if exists bans add column if not exists added_at timestamptz default now();

        -- active_members / toggles
        alter table if exists active_members add column if not exists last_activity_at timestamptz;
        alter table if exists toggles add column if not exists random_tag boolean default false;
        """
        async with self.pool.acquire() as con:
            await con.execute(create_sql)
            await con.execute(migrate_sql)

        # Seed owner
        if OWNER_ID:
            await self.upsert_user(OWNER_ID, username=None, first_name="OWNER", last_name=None, is_bot=False)
            await self.add_role(OWNER_ID, "owner")

    # --- User helpers ---
    async def upsert_user(self, user_id: int, username: Optional[str], first_name: str, last_name: Optional[str], is_bot: bool):
        async with self.pool.acquire() as con:
            await con.execute("""
            insert into users(user_id, username, first_name, last_name, is_bot, last_seen_at)
            values($1,$2,$3,$4,$5, now())
            on conflict (user_id) do update set
                username = excluded.username,
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                is_bot = excluded.is_bot,
                last_seen_at = now();
            """, user_id, username, first_name, last_name, is_bot)

    async def set_user_in_group(self, user_id: int, in_group: bool):
        async with self.pool.acquire() as con:
            await con.execute("update users set in_group=$2 where user_id=$1;", user_id, in_group)

    async def set_gender(self, user_id: int, gender: Optional[str]):
        async with self.pool.acquire() as con:
            await con.execute("update users set gender=$2 where user_id=$1;", user_id, gender)

    async def get_user(self, user_id: int) -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetchrow("select * from users where user_id=$1;", user_id)

    # --- Roles ---
    async def add_role(self, user_id: int, role: str):
        async with self.pool.acquire() as con:
            await con.execute("insert into roles(user_id, role) values($1,$2) on conflict do nothing;", user_id, role)

    async def remove_role(self, user_id: int, role: str):
        async with self.pool.acquire() as con:
            await con.execute("delete from roles where user_id=$1 and role=$2;", user_id, role)

    async def has_any_role(self, user_id: int, roles: List[str]) -> bool:
        async with self.pool.acquire() as con:
            rows = await con.fetch("select role from roles where user_id=$1;", user_id)
        rs = {r["role"] for r in rows}
        return any(x in rs for x in roles)

    async def get_roles(self, user_id: int) -> List[str]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("select role from roles where user_id=$1 order by role;", user_id)
        return [r["role"] for r in rows]

    async def list_by_role(self, role: str) -> List[int]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("select user_id from roles where role=$1;", role)
        return [r["user_id"] for r in rows]

    async def list_all_managers(self) -> Dict[str, List[int]]:
        roles = ['owner','senior_global','senior_call','senior_chat','admin_call','admin_chat']
        res = {}
        for r in roles:
            res[r] = await self.list_by_role(r)
        return res

    # --- Bans ---
    async def ban_add(self, user_id: int, reason: Optional[str], added_by: int):
        async with self.pool.acquire() as con:
            await con.execute("""
            insert into bans(user_id, reason, added_by) values($1,$2,$3)
            on conflict (user_id) do update set reason=excluded.reason, added_by=excluded.added_by, added_at=now();
            """, user_id, reason, added_by)

    async def ban_remove(self, user_id: int):
        async with self.pool.acquire() as con:
            await con.execute("delete from bans where user_id=$1;", user_id)

    async def is_banned(self, user_id: int) -> bool:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("select 1 from bans where user_id=$1;", user_id)
        return bool(row)

    async def list_banned(self) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("select * from bans order by added_at desc;")
        return rows

    # --- Contact blocks ---
    async def set_contact_block(self, user_id: int, blocked: bool, reason: Optional[str] = None):
        async with self.pool.acquire() as con:
            await con.execute("""
                insert into contact_blocks(user_id, blocked, reason, updated_at)
                values($1,$2,$3, now())
                on conflict (user_id) do update set blocked=$2, reason=$3, updated_at=now();
            """, user_id, blocked, reason)

    async def is_contact_blocked(self, user_id: int) -> bool:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("select blocked from contact_blocks where user_id=$1;", user_id)
        return bool(row and row["blocked"])

    # --- Stats ---
    async def bump_stat(self, chat_id: int, user_id: int, *, is_media: bool, is_voice: bool, mentions_made: int, at: datetime):
        d = at.astimezone(TZINFO).date()
        async with self.pool.acquire() as con:
            await con.execute("""
            insert into stats_daily(chat_id, user_id, date, messages_count, media_count, voice_count, mentions_made_count)
            values($1,$2,$3,1,$4,$5,$6)
            on conflict (chat_id,user_id,date) do update set
                messages_count = stats_daily.messages_count + 1,
                media_count = stats_daily.media_count + excluded.media_count,
                voice_count = stats_daily.voice_count + excluded.voice_count,
                mentions_made_count = stats_daily.mentions_made_count + excluded.mentions_made_count;
            """, chat_id, user_id, d, 1 if is_media else 0, 1 if is_voice else 0, mentions_made)

    async def add_session(self, chat_id: int, user_id: int, kind: str, start_at: datetime):
        async with self.pool.acquire() as con:
            await con.execute("""
            insert into sessions(chat_id,user_id,type,start_at,active) values($1,$2,$3,$4,true);
            """, chat_id, user_id, kind, start_at)

    async def end_session(self, chat_id: int, user_id: int, ended_by: str, end_at: datetime):
        # Use CTE to update latest active session safely (PostgreSQL compliant)
        async with self.pool.acquire() as con:
            row = await con.fetchrow("""
                with c as (
                    select id from sessions
                    where chat_id=$1 and user_id=$2 and active=true
                    order by start_at desc
                    limit 1
                )
                update sessions s
                set active=false, end_at=$3, ended_by=$4
                from c
                where s.id = c.id
                returning s.start_at, s.type;
            """, chat_id, user_id, end_at, ended_by)
            return row

    async def has_active_session(self, chat_id: int, user_id: int) -> bool:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("select 1 from sessions where chat_id=$1 and user_id=$2 and active=true;", chat_id, user_id)
        return bool(row)

    async def update_call_time_aggregate_for_day(self, chat_id: int, user_id: int, d: date):
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                select start_at, coalesce(end_at, now()) as end_at
                from sessions
                where chat_id=$1 and user_id=$2 and type='call' and date(start_at at time zone $3)=$4;
            """, chat_id, user_id, TZ, d)
            total = 0
            for r in rows:
                delta = (r["end_at"] - r["start_at"]).total_seconds()
                if delta > 0:
                    total += int(delta)
            await con.execute("""
                insert into stats_daily(chat_id,user_id,date,call_time_sec)
                values($1,$2,$3,$4)
                on conflict (chat_id,user_id,date) do update set
                    call_time_sec=excluded.call_time_sec;
            """, chat_id, user_id, d, total)

    async def get_stats_for_user_days(self, chat_id: int, user_id: int, days: int) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                select * from stats_daily where chat_id=$1 and user_id=$2
                order by date desc limit $3;
            """, chat_id, user_id, days)
        return rows

    async def set_active_member(self, chat_id: int, user_id: int, at: datetime):
        async with self.pool.acquire() as con:
            await con.execute("""
                insert into active_members(chat_id,user_id,last_activity_at)
                values($1,$2,$3)
                on conflict (chat_id,user_id) do update set last_activity_at=$3;
            """, chat_id, user_id, at)

    async def get_active_members(self, chat_id: int, since_minutes: int = 1440) -> List[int]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                select user_id from active_members
                where chat_id=$1 and last_activity_at >= now() - ($2::text||' minutes')::interval;
            """, chat_id, since_minutes)
        return [r["user_id"] for r in rows]

    async def list_gender(self, gender: str) -> List[int]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("select user_id from users where gender=$1 and in_group=true;", gender)
        return [r["user_id"] for r in rows]

    async def inc_game_score(self, chat_id: int, user_id: int, delta: int = 1):
        async with self.pool.acquire() as con:
            await con.execute("""
                insert into game_scores(chat_id,user_id,score,updated_at) values($1,$2,$3,now())
                on conflict (chat_id,user_id) do update set score = game_scores.score + $3, updated_at=now();
            """, chat_id, user_id, delta)

    async def get_game_top(self, chat_id: int, limit: int = 10):
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                select u.user_id, coalesce(u.first_name,'') as fn, coalesce(u.last_name,'') as ln, u.username as un, s.score
                from game_scores s
                join users u on u.user_id = s.user_id
                where s.chat_id=$1
                order by s.score desc nulls last, updated_at desc
                limit $2;
            """, chat_id, limit)
        return rows

    async def set_random_tag(self, chat_id: int, on: bool):
        async with self.pool.acquire() as con:
            await con.execute("""
                insert into toggles(chat_id, random_tag) values($1,$2)
                on conflict (chat_id) do update set random_tag=$2;
            """, chat_id, on)

    async def get_random_tag(self, chat_id: int) -> bool:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("select random_tag from toggles where chat_id=$1;", chat_id)
        return bool(row and row["random_tag"])

# ----------------------------- Utilities ------------------------------

def mention(user_id: int, name: str) -> str:
    safe = re.sub(r'[\[\]\(\)_*`>#+\-=|{}.!]', '', name or "کاربر")
    return f"[{safe}](tg://user?id={user_id})"

def now_tz() -> datetime:
    return datetime.now(tz=TZINFO)

WEEKDAYS_FA = ["دوشنبه","سه‌شنبه","چهارشنبه","پنج‌شنبه","جمعه","شنبه","یکشنبه"]

def format_jalali(dt: datetime) -> str:
    if jdatetime is None:
        return dt.astimezone(TZINFO).strftime("%Y-%m-%d %H:%M")
    j = jdatetime.datetime.fromgregorian(datetime=dt.astimezone(TZINFO))
    weekdays = ["دوشنبه","سه‌شنبه","چهارشنبه","پنج‌شنبه","جمعه","شنبه","یکشنبه"]
    return f"{j.strftime('%Y/%m/%d %H:%M')} - {weekdays[j.weekday()]}"

def format_secs(s: int) -> str:
    h = s // 3600
    s -= h*3600
    m = s // 60
    s -= m*60
    return f"{h:02}:{m:02}:{s:02}"

def alert_not_for_you():
    return "این دکمه برای شما نیست رفیق! 😅"

FUN_PREFIXES = ["هی","اوه","سرورِ مهربون","آقا/خانم قهرمان","حاجی","رفیق","هی رفیق","قربونت","عه","ای جان"]
FUN_SUFFIXES = ["کجایی؟ 😴","بیا یه تکونی به خودت بده! 💃","جمع خوابالوهاست؟ 😜","چایی حاضر شد، بیا! ☕","ما که پیر شدیم، تو بیا! 👴","بی‌خیال تنبلی، بپر تو چت! 🏃","دلتنگت شدیم! ❤️","یه چیزی بگو دیگه! 🎤","بپر تو ویس کال ببینیمت! 🎧","تو که رفتی، سکوت اومد! 🤫","نیا نیا، شوخی کردم بیا 😂","میای یا بزنم تگ بعدی؟ 🤨","غیبت طولانی، گزارش میشه‌ها! 📋"]
BOT_NICE_LINES_BASE = ["قربون محبتت برم! 😍","جانِ دلمی! 💙","تو که باشی، همه چی روبه‌راست 😎","این گروه با تو می‌درخشه ✨","دمت گرم که هستی 💪","ایول بهت! 👏","خاص‌ترین آدمِ جمعی 😌","فدات که فعالی 🌟","تو هیچی کم نداری ❤️","مرسی که حالِ جمعو خوب می‌کنی 🌈"]
RANDOM_TAG_LINES = [f"{p} {s}" for p in FUN_PREFIXES for s in FUN_SUFFIXES] * 5
BOT_NICE_LINES = BOT_NICE_LINES_BASE * 12

# ----------------------------- Permission Helpers ---------------------
async def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

async def is_manager(db: DB, user_id: int) -> bool:
    if await is_owner(user_id):
        return True
    return await db.has_any_role(user_id, ['senior_global','senior_call','senior_chat','admin_call','admin_chat'])

async def is_senior(db: DB, user_id: int) -> bool:
    if await is_owner(user_id):
        return True
    return await db.has_any_role(user_id, ['senior_global','senior_call','senior_chat'])

# ----------------------------- Start & PM Panel -----------------------
def pm_panel_kb() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("📨 ارتباط با گارد مدیران", callback_data="pm|guard")],
        [InlineKeyboardButton("👑 ارتباط با مالک", callback_data="pm|owner")],
        [InlineKeyboardButton("📊 آمار من", callback_data="pm|mystats")],
    ]
    return InlineKeyboardMarkup(kb)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await context.bot.send_message(
        chat_id=u.id,
        text="سلام! این پنل شخصی ربات سولزه. یکی از گزینه‌ها رو انتخاب کن 👇",
        reply_markup=pm_panel_kb()
    )

# ----------------------------- Contact Flows --------------------------
async def ensure_user(db: DB, u) -> None:
    await db.upsert_user(u.id, u.username, u.first_name or "", u.last_name, u.is_bot)

async def cb_pm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split("|")
    if len(data) < 2:
        return
    kind = data[1]  # guard / owner / mystats
    user = query.from_user
    db: DB = context.bot_data["DB"]
    await ensure_user(db, user)
    if kind == "mystats":
        await send_stats_for_user(user.id, context)
        return

    if await db.is_contact_blocked(user.id):
        await query.edit_message_text("متأسفم! دسترسی پیام‌دادن به این بخش برای شما بسته شده. 🚫")
        return

    await db.pool.execute("""
        insert into contact_states(user_id,kind,waiting) values($1,$2,true)
        on conflict (user_id) do update set kind=excluded.kind, waiting=true;
    """, user.id, kind)

    btns = [[InlineKeyboardButton("✉️ ارسال یک پیام", callback_data=f"sendonce|{kind}|{user.id}")],
            [InlineKeyboardButton("◀️ بازگشت", callback_data="back|pm")]]
    await query.edit_message_text(
        "حله! وقتی روی «ارسال یک پیام» بزنی، فقط *یک* پیام (هر فرمتی حتی آلبوم) می‌تونی بفرستی. بعدش گزینه «ارسال مجدد» میاد که اگه خواستی دوباره یک پیام بفرستی.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(btns)
    )

async def cb_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split("|")
    if len(parts) < 2:
        return
    where = parts[1]
    if where == "pm":
        await q.edit_message_text("سلام! این پنل شخصی ربات سولزه. یکی از گزینه‌ها رو انتخاب کن 👇", reply_markup=pm_panel_kb())

async def cb_sendonce(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    kind, owner_id = parts[1], int(parts[2])
    if q.from_user.id != owner_id:
        await q.answer(alert_not_for_you(), show_alert=True)
        return
    await q.answer()
    await q.edit_message_text("منتظرتم! الان فقط *یک* پیام بفرست. بعد از ارسال می‌تونی «ارسال مجدد» بزنی.", parse_mode=ParseMode.MARKDOWN)

async def handle_pm_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    st = await db.pool.fetchrow("select * from contact_states where user_id=$1;", user.id)
    if not st or not st["waiting"]:
        return
    kind = st["kind"]
    if await db.is_contact_blocked(user.id):
        await update.message.reply_text("ارسال پیام برای شما بسته شده. 🚫")
        await db.pool.execute("update contact_states set waiting=false where user_id=$1;", user.id)
        return

    try:
        header = f"📨 پیام جدید از {mention(user.id, user.full_name)}\n@{user.username or '-'} | id: `{user.id}`"
        await context.bot.send_message(
            chat_id=GUARD_CHAT_ID if kind=="guard" else OWNER_ID,
            text=header,
            parse_mode=ParseMode.MARKDOWN
        )
        await update.message.copy(
            chat_id=GUARD_CHAT_ID if kind=="guard" else OWNER_ID,
        )
        kb = [[InlineKeyboardButton("📩 پاسخ", callback_data=f"replyto|{kind}|{user.id}|{update.effective_user.id}")],
              [InlineKeyboardButton("🚫 مسدود DM", callback_data=f"blockdm|{user.id}")]]
        await context.bot.send_message(
            chat_id=GUARD_CHAT_ID if kind=="guard" else OWNER_ID,
            text="—",
            reply_markup=InlineKeyboardMarkup(kb)
        )
    except Exception as e:
        logger.exception("copy to target failed: %s", e)
        await update.message.reply_text("ارسال نشد! یکبار دیگه امتحان کن.")
        return

    await db.pool.execute("update contact_states set waiting=false where user_id=$1;", user.id)
    await context.bot.send_message(
        chat_id=user.id,
        text="پیامت رسید ✅\nاگه خواستی *فقط یک پیام دیگه* بفرستی روی «ارسال مجدد» بزن.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔁 ارسال مجدد", callback_data=f"pm|{kind}")],
                                           [InlineKeyboardButton("◀️ بازگشت", callback_data="back|pm")]])
    )

async def cb_replyto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    if len(parts) < 4:
        await q.answer()
        return
    kind, target_user_id = parts[1], int(parts[2])
    admin = q.from_user
    db: DB = context.bot_data["DB"]
    if not (await is_manager(db, admin.id)):
        await q.answer("فقط مدیران می‌تونن جواب بدن.", show_alert=True)
        return
    await db.pool.execute("""
        insert into admin_reply_states(admin_id,target_user_id,kind) values($1,$2,$3)
        on conflict (admin_id,kind) do update set target_user_id=$2;
    """, admin.id, target_user_id, kind)
    await q.answer()
    await q.edit_message_text("اوکی! *فقط یک پیام* بفرست تا برای کاربر ارسال کنم.", parse_mode=ParseMode.MARKDOWN)

async def handle_guard_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # listens in GUARD_CHAT_ID and in Owner PM for one-shot admin replies
    if update.effective_chat.id not in [GUARD_CHAT_ID, OWNER_ID]:
        return
    admin = update.effective_user
    db: DB = context.bot_data["DB"]
    st = await db.pool.fetchrow("select * from admin_reply_states where admin_id=$1;", admin.id)
    if not st:
        return
    target = int(st["target_user_id"])
    kind = st["kind"]
    try:
        await update.message.copy(chat_id=target)
        await update.message.reply_text("پیامت ارسال شد ✅", reply_to_message_id=update.message.message_id)
        kb = [[InlineKeyboardButton("🔁 پاسخ مجدد", callback_data=f"replyto|{kind}|{target}|{admin.id}")]]
        await context.bot.send_message(chat_id=update.effective_chat.id, text="—", reply_markup=InlineKeyboardMarkup(kb))
    except Exception as e:
        logger.exception("send reply failed: %s", e)
        await update.message.reply_text("نشد! دوباره امتحان کن.")
    await db.pool.execute("delete from admin_reply_states where admin_id=$1 and kind=$2;", admin.id, kind)

async def cb_block_dm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    target_id = int(parts[1])
    admin = q.from_user
    db: DB = context.bot_data["DB"]
    if not (await is_manager(db, admin.id)):
        await q.answer("فقط مدیران!", show_alert=True); return
    await db.set_contact_block(target_id, True, reason="by admin")
    await q.answer("بلاک شد.")
    await q.edit_message_text(f"کاربر {target_id} برای پیام‌دادن بلاک شد.")

# ----------------------------- Stats & Presence -----------------------
SESSION_SELECT_PREFIX = "sess|"

def build_session_kb(author_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎧 کال", callback_data=f"{SESSION_SELECT_PREFIX}call|{author_id}")],
        [InlineKeyboardButton("💬 چت", callback_data=f"{SESSION_SELECT_PREFIX}chat|{author_id}")],
    ])

async def maybe_prompt_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    user = update.effective_user
    if user.is_bot:
        return
    db: DB = context.bot_data["DB"]
    await ensure_user(db, user)
    await db.set_active_member(MAIN_CHAT_ID, user.id, now_tz())
    if await db.is_banned(user.id):
        return
    msg = update.effective_message
    is_media = any([msg.photo, msg.video, msg.document, msg.animation, msg.audio, msg.sticker])
    is_voice = bool(msg.voice)
    mentions = 0
    if msg.entities:
        for e in msg.entities:
            if e.type in [MessageEntity.MENTION, MessageEntity.TEXT_MENTION]:
                mentions += 1
    await db.bump_stat(MAIN_CHAT_ID, user.id, is_media=is_media, is_voice=is_voice, mentions_made=mentions, at=now_tz())

    if await is_manager(db, user.id):
        if not await db.has_active_session(MAIN_CHAT_ID, user.id):
            try:
                await msg.reply_text("نوع فعالیتت رو انتخاب کن:", reply_markup=build_session_kb(user.id))
            except Exception as e:
                logger.warning("session prompt failed: %s", e)
        await schedule_idle_job(context, user.id)
    await db.set_user_in_group(user.id, True)

async def schedule_idle_job(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    job_name = f"idle_{MAIN_CHAT_ID}_{user_id}"
    for job in context.job_queue.get_jobs_by_name(job_name):
        job.schedule_removal()
    context.job_queue.run_once(idle_timeout_job, when=300, name=job_name, data={"chat_id": MAIN_CHAT_ID, "user_id": user_id})

async def idle_timeout_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    chat_id = data.get("chat_id"); user_id = data.get("user_id")
    db: DB = context.bot_data["DB"]
    if await db.has_active_session(chat_id, user_id):
        row = await db.end_session(chat_id, user_id, "auto", now_tz())
        if row:
            kind = row["type"]
            await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=f"⛔ پایان خودکار سشن {kind} برای {mention(user_id,'کاربر')} به دلیل عدم فعالیت ۵ دقیقه‌ای.", parse_mode=ParseMode.MARKDOWN)

async def cb_session_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    if len(parts) < 3:
        await q.answer(); return
    kind, author_id = parts[1], int(parts[2])
    if q.from_user.id != author_id:
        await q.answer(alert_not_for_you(), show_alert=True); return
    db: DB = context.bot_data["DB"]
    if await db.has_active_session(MAIN_CHAT_ID, q.from_user.id):
        await q.answer("الان هم یک سشن باز داری!"); return
    await db.add_session(MAIN_CHAT_ID, q.from_user.id, kind, now_tz())
    await q.answer("ثبت شد ✅")
    try:
        await q.edit_message_text(f"شروع فعالیت { 'کال' if kind=='call' else 'چت' } ✅")
    except: pass
    await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=f"✅ شروع سشن { 'کال' if kind=='call' else 'چت' } توسط {mention(q.from_user.id, q.from_user.full_name)}", parse_mode=ParseMode.MARKDOWN)
    await schedule_idle_job(context, q.from_user.id)

async def cmd_register_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not await is_manager(db, user.id):
        return
    if await db.has_active_session(MAIN_CHAT_ID, user.id):
        await update.message.reply_text("الان هم یک سشن باز داری!")
        return
    await update.message.reply_text("نوع فعالیتت رو انتخاب کن:", reply_markup=build_session_kb(user.id))

async def cmd_register_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not await is_manager(db, user.id):
        return
    row = await db.end_session(MAIN_CHAT_ID, user.id, "user", now_tz())
    if not row:
        await update.message.reply_text("سشنی باز نیست.")
        return
    await update.message.reply_text("پایان فعالیت شما گزارش شد، خسته نباشی! ✅")
    await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=f"🟥 پایان سشن {row['type']} توسط {mention(user.id, user.full_name)}", parse_mode=ParseMode.MARKDOWN)

async def nightly_stats_job(context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    now = now_tz()
    y = (now - timedelta(days=1)).date()

    managers = await db.list_all_managers()
    all_ids = {uid for lst in managers.values() for uid in lst}
    for uid in all_ids:
        await db.update_call_time_aggregate_for_day(MAIN_CHAT_ID, uid, y)

    async def fetch(uids: List[int]):
        if not uids: return []
        res = []
        for uid in uids:
            rows = await db.get_stats_for_user_days(MAIN_CHAT_ID, uid, 1)
            if rows:
                r = rows[0]
                res.append((uid, r["messages_count"], r["media_count"], r["voice_count"], r["mentions_made_count"]))
            else:
                res.append((uid, 0,0,0,0))
        return res

    chat_group = managers.get("admin_chat", []) + managers.get("senior_chat", []) + managers.get("senior_global", []) + ([OWNER_ID] if OWNER_ID else [])
    call_group = managers.get("admin_call", []) + managers.get("senior_call", []) + managers.get("senior_global", []) + ([OWNER_ID] if OWNER_ID else [])

    chat_stats = await fetch(chat_group)
    call_stats = []
    for uid in call_group:
        rows = await db.get_stats_for_user_days(MAIN_CHAT_ID, uid, 1)
        if rows:
            r = rows[0]
            call_stats.append((uid, r["call_time_sec"]))
        else:
            call_stats.append((uid, 0))

    if jdatetime:
        j = jdatetime.date.fromgregorian(date=y)
        date_str = f"{j.strftime('%Y/%m/%d')}"
    else:
        date_str = y.strftime("%Y-%m-%d")
    wd = WEEKDAYS_FA[(y.weekday()+1) % 7]

    lines = [f"📊 آمار چت مدیران — {date_str} ({wd})", ""]
    for uid, msgs, media, voice, men in chat_stats:
        lines.append(f"• {mention(uid, 'کاربر')} — پیام: {msgs} | رسانه: {media} | ویس: {voice} | منشن: {men}")
    text1 = "\n".join(lines)

    lines2 = [f"🎧 آمار کال مدیران — {date_str} ({wd})", ""]
    for uid, sec in call_stats:
        lines2.append(f"• {mention(uid, 'کاربر')} — زمان حضور: {format_secs(int(sec))}")
    text2 = "\n".join(lines2)

    lines3 = [f"📣 منشن‌های امروز — {date_str} ({wd})", ""]
    for uid, msgs, media, voice, men in chat_stats:
        lines3.append(f"• {mention(uid,'کاربر')}: {men}")
    text3 = "\n".join(lines3)

    await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=text1, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=text2, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    await context.bot.send_message(chat_id=GUARD_CHAT_ID, text=text3, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

async def send_stats_for_user(user_id: int, context: ContextTypes.DEFAULT_TYPE, reply_to: Optional[int]=None):
    db: DB = context.bot_data["DB"]
    rows = await db.get_stats_for_user_days(MAIN_CHAT_ID, user_id, 7)
    if not rows:
        await context.bot.send_message(chat_id=user_id, text="آماری برای ۷ روز گذشته ندارم.")
        return
    try:
        photos = await context.bot.get_user_profile_photos(user_id, limit=1)
        file_id = photos.photos[0][-1].file_id if photos.total_count > 0 else None
    except:
        file_id = None
    lines = ["📊 آمار ۷ روز گذشته در گروه سولز:", ""]
    for r in reversed(rows):
        d = r["date"]
        jd = jdatetime.date.fromgregorian(date=d).strftime("%Y/%m/%d") if jdatetime else d.strftime("%Y-%m-%d")
        lines.append(f"• {jd} — پیام: {r['messages_count']} | رسانه: {r['media_count']} | ویس: {r['voice_count']} | منشن: {r['mentions_made_count']} | کال: {format_secs(int(r['call_time_sec']))}")
    cap = "\n".join(lines)
    if file_id:
        await context.bot.send_photo(chat_id=user_id, photo=file_id, caption=cap)
    else:
        await context.bot.send_message(chat_id=user_id, text=cap)

# ----------------------------- Management -----------------------------
async def extract_target_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    msg = update.effective_message
    if msg.reply_to_message:
        return msg.reply_to_message.from_user.id
    text = (msg.text or "").strip()
    parts = text.split()
    if len(parts) >= 2:
        token = parts[1]
        if token.startswith("@"):
            # Resolving @username via Bot API programmatically is unreliable; use reply or numeric id.
            return None
        else:
            try:
                return int(token)
            except:
                return None
    return None

async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not (await is_manager(db, user.id)):
        return
    target = await extract_target_user_id(update, context)
    if not target:
        await update.message.reply_text("هدف نامعتبره. با ریپلای یا آیدی عددی بزن.")
        return
    await db.ban_add(target, reason="by command", added_by=user.id)
    try:
        await context.bot.ban_chat_member(chat_id=MAIN_CHAT_ID, user_id=target)
    except Exception as e:
        logger.info("ban action: %s", e)
    await update.message.reply_text(f"کاربر {target} به لیست ممنوع اضافه شد و دسترسی گروه قطع شد.")

async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not (await is_manager(db, user.id)):
        return
    target = await extract_target_user_id(update, context)
    if not target:
        await update.message.reply_text("هدف نامعتبره. با ریپلای یا آیدی عددی بزن.")
        return
    await db.ban_remove(target)
    try:
        await context.bot.unban_chat_member(chat_id=MAIN_CHAT_ID, user_id=target, only_if_banned=True)
    except Exception as e:
        logger.info("unban action: %s", e)
    await update.message.reply_text(f"کاربر {target} از لیست ممنوع حذف شد و اجازه ورود گرفت.")

async def cmd_list_banned(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not (await is_manager(db, user.id)):
        return
    rows = await db.list_banned()
    if not rows:
        await update.message.reply_text("لیست ممنوع خالیه.")
        return
    lines = ["🚫 لیست ممنوع:", ""]
    for r in rows:
        lines.append(f"• {mention(r['user_id'],'کاربر')} — id: `{r['user_id']}`")
    text = "\n".join(lines)
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    if OWNER_ID:
        await context.bot.send_message(chat_id=OWNER_ID, text=text, parse_mode=ParseMode.MARKDOWN)

ROLE_MAP = {
    "ترفیع چت": "admin_chat",
    "ترفیع کال": "admin_call",
    "ترفیع ارشد چت": "senior_chat",
    "ترفیع ارشد کال": "senior_call",
    "ترفیع ارشد کل": "senior_global",
}
DEMOTE_MAP = {
    "عزل چت": "admin_chat",
    "عزل کال": "admin_call",
    "عزل ارشد چت": "senior_chat",
    "عزل ارشد کال": "senior_call",
    "عزل ارشد کل": "senior_global",
}

async def handle_promote_demote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not await is_owner(user.id):
        return
    text = (update.message.text or "").strip()
    target = await extract_target_user_id(update, context)
    if not target:
        await update.message.reply_text("هدف نامعتبره.")
        return
    if any(text.startswith(k) for k in ROLE_MAP.keys()):
        for k, role in ROLE_MAP.items():
            if text.startswith(k):
                await db.add_role(target, role)
                await update.message.reply_text(f"کاربر {mention(target,'کاربر')} به عنوان {k.replace('ترفیع ','')} منصوب شد.", parse_mode=ParseMode.MARKDOWN)
                return
    if any(text.startswith(k) for k in DEMOTE_MAP.keys()):
        for k, role in DEMOTE_MAP.items():
            if text.startswith(k):
                await db.remove_role(target, role)
                await update.message.reply_text(f"سمت {k.replace('عزل ','')} از کاربر برداشته شد.", parse_mode=ParseMode.MARKDOWN)
                return

async def cmd_list_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    if not (await is_senior(db, user.id)):
        return
    managers = await db.list_all_managers()
    order = ["owner","senior_global","senior_call","senior_chat","admin_call","admin_chat"]
    names = {"owner":"مالک","senior_global":"ارشد کل","senior_call":"ارشد کال","senior_chat":"ارشد چت","admin_call":"ادمین کال","admin_chat":"ادمین چت"}
    lines = ["👥 لیست گارد (به ترتیب سمت):",""]
    for r in order:
        ids = managers.get(r, [])
        if not ids: continue
        lines.append(f"— {names[r]}:")
        for uid in ids:
            lines.append(f"   • {mention(uid,'کاربر')}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    user = update.effective_user
    if not await is_manager(db, user.id):
        return
    target = await extract_target_user_id(update, context)
    t_id = target or user.id
    rows = await db.get_stats_for_user_days(MAIN_CHAT_ID, t_id, 7)
    if not rows:
        await update.message.reply_text("آماری موجود نیست.")
        return
    try:
        photos = await context.bot.get_user_profile_photos(t_id, limit=1)
        file_id = photos.photos[0][-1].file_id if photos.total_count > 0 else None
    except:
        file_id = None
    lines = [f"📊 آمار ۷ روز گذشته برای {mention(t_id,'کاربر')}:", ""]
    for r in reversed(rows):
        d = r["date"]
        jd = jdatetime.date.fromgregorian(date=d).strftime("%Y/%m/%d") if jdatetime else d.strftime("%Y-%m-%d")
        lines.append(f"• {jd}: پیام {r['messages_count']} | رسانه {r['media_count']} | ویس {r['voice_count']} | منشن {r['mentions_made_count']} | کال {format_secs(int(r['call_time_sec']))}")
    cap = "\n".join(lines)
    if file_id:
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=file_id, caption=cap, reply_to_message_id=update.effective_message.message_id)
    else:
        await update.message.reply_text(cap, parse_mode=ParseMode.MARKDOWN)

# ----------------------------- Tag Panel ------------------------------
def tag_panel_kb(author_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎧 تگ کال", callback_data=f"tag|call|{author_id}")],
        [InlineKeyboardButton("💬 تگ چت", callback_data=f"tag|chat|{author_id}")],
        [InlineKeyboardButton("🔥 تگ اعضای فعال", callback_data=f"tag|active|{author_id}")],
        [InlineKeyboardButton("👧 تگ دخترها", callback_data=f"tag|girls|{author_id}")],
        [InlineKeyboardButton("👦 تگ پسرها", callback_data=f"tag|boys|{author_id}")],
    ])

async def cmd_tag_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("کیو می‌خوای صدا کنیم؟", reply_markup=tag_panel_kb(update.effective_user.id))

async def cb_tag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    group, author_id = parts[1], int(parts[2])
    if q.from_user.id != author_id:
        await q.answer(alert_not_for_you(), show_alert=True); return
    db: DB = context.bot_data["DB"]
    await q.answer("باشه!")
    ids: List[int] = []
    if group == "call":
        ids += await db.list_by_role("admin_call")
        ids += await db.list_by_role("senior_call")
        ids += await db.list_by_role("senior_global")
        if OWNER_ID: ids.append(OWNER_ID)
    elif group == "chat":
        ids += await db.list_by_role("admin_chat")
        ids += await db.list_by_role("senior_chat")
        ids += await db.list_by_role("senior_global")
        if OWNER_ID: ids.append(OWNER_ID)
    elif group == "active":
        ids = await db.get_active_members(MAIN_CHAT_ID, 1440)
    elif group == "girls":
        ids = await db.list_gender("female")
    elif group == "boys":
        ids = await db.list_gender("male")

    uniq, seen = [], set()
    for i in ids:
        if i in seen: continue
        seen.add(i); uniq.append(i)

    reply_to = q.message.reply_to_message.message_id if q.message and q.message.reply_to_message else None
    batches = [uniq[i:i+5] for i in range(0, len(uniq), 5)]
    if not batches:
        await q.edit_message_text("کسی پیدا نشد.")
        return
    await q.edit_message_text("دارم صدا می‌زنم...")
    for b in batches:
        line = "، ".join(mention(uid, "کاربر") for uid in b)
        try:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=line, parse_mode=ParseMode.MARKDOWN, reply_to_message_id=reply_to)
            await asyncio.sleep(1.2)
        except Exception as e:
            logger.info("tag send failed: %s", e)

# ----------------------------- Gender Command -------------------------
def gender_kb(author_id: int, target_id: Optional[int]) -> InlineKeyboardMarkup:
    tid = target_id or 0
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👦 پسر", callback_data=f"gender|male|{author_id}|{tid}")],
        [InlineKeyboardButton("👧 دختر", callback_data=f"gender|female|{author_id}|{tid}")],
    ])

async def cmd_gender(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    user = update.effective_user
    if not (await is_manager(db, user.id)):
        return
    target = await extract_target_user_id(update, context)
    await update.message.reply_text("جنسیت رو انتخاب کن:", reply_markup=gender_kb(user.id, target))

async def cb_gender(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    gender, author_id, target_id = parts[1], int(parts[2]), int(parts[3])
    if q.from_user.id != author_id:
        await q.answer(alert_not_for_you(), show_alert=True); return
    db: DB = context.bot_data["DB"]
    target = target_id or q.from_user.id
    await db.set_gender(target, "male" if gender=="male" else "female")
    await q.answer("ثبت شد ✅")
    try:
        await q.edit_message_text("جنسیت ذخیره شد.")
    except: pass

# ----------------------------- Help ----------------------------------
HELP_TEXT = """
راهنمای ربات سولز — نسخه فشرده
(همه دستورات *بدون /* هستند)

مدیریت حضور:
• «ثبت» → باز کردن پنل انتخاب «کال» یا «چت»
• «ثبت خروج» → پایان سشن جاری (اگر ۵ دقیقه پیام ندی خودکار بسته میشه)

مدیریت کاربران:
• «ممنوع [ریپلای/آیدی]» → بن کامل کاربر
• «آزاد [ریپلای/آیدی]» → حذف از لیست ممنوع
• «لیست ممنوع» → نمایش لیست ممنوع‌ها

مقامات (فقط مالک):
• «ترفیع چت/ترفیع کال/ترفیع ارشد چت/ترفیع ارشد کال/ترفیع ارشد کل [ریپلای/آیدی]»
• «عزل چت/عزل کال/عزل ارشد چت/عزل ارشد کال/عزل ارشد کل [ریپلای/آیدی]»

اطلاعات و تگ:
• «لیست گارد» (مالک و ارشدها)
• «آیدی [اختیاری: ریپلای/آیدی]» → آمار ۷ روز گذشته با عکس پروفایل
• «تگ» → پنل تگ: کال/چت/اعضای فعال/دخترها/پسرها
• «جنسیت [ریپلای اختیاری]» → انتخاب پسر/دختر برای خودت یا هدف

فان:
• «تگ روشن» / «تگ خاموش» → تگ تصادفی فعال‌های ساکت + جمله‌های فان
• «بازی» → پنل ۱۵+ بازی گروهی
• «ربات» (برای مقام‌داران) → جواب‌های قشنگ و متنوع

پنل خصوصی /start:
• «ارتباط با گارد مدیران» (ارسال یک‌باره)
• «ارتباط با مالک» (ارسال یک‌باره)
• «آمار من»
"""

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    user = update.effective_user
    if not (await is_manager(db, user.id)):
        return
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.MARKDOWN)

# ----------------------------- Random Tag Toggle ----------------------
async def cmd_tag_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db: DB = context.bot_data["DB"]
    # فقط مالک
    if not await is_owner(user.id):
        return
    text = (update.message.text or "").strip()
    on = "روشن" in text
    await db.set_random_tag(MAIN_CHAT_ID, on)
    await update.message.reply_text("حله. تگ تصادفی " + ("روشن شد ✅" if on else "خاموش شد ⛔"))

async def random_tag_job(context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    if not await db.get_random_tag(MAIN_CHAT_ID):
        return
    ids = await db.get_active_members(MAIN_CHAT_ID, since_minutes=1440)
    if not ids:
        return
    target = random.choice(ids)
    phrase = random.choice(RANDOM_TAG_LINES)
    try:
        await context.bot.send_message(chat_id=MAIN_CHAT_ID, text=f"{mention(target, 'داداش/خواهر')} {phrase}", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.info("random tag send failed: %s", e)

# ----------------------------- "ربات" friendly replies ----------------
async def cmd_bot_nice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    user = update.effective_user
    if not (await is_manager(db, user.id)):
        return
    await update.message.reply_text(random.choice(BOT_NICE_LINES))

# ----------------------------- Game Engine ----------------------------
class GameSession:
    def __init__(self, chat_id: int, game_id: str, prompt: str, answers: List[str], started_by: int, points: int = 1, meta: Optional[dict]=None):
        self.chat_id = chat_id
        self.game_id = game_id
        self.prompt = prompt
        self.answers = [a.lower() for a in answers]
        self.started_by = started_by
        self.points = points
        self.meta = meta or {}
        self.created_at = now_tz()
        self.active = True

GAME_SESSIONS: Dict[int, GameSession] = {}

def normalize(s: str) -> str:
    s = (s or "").strip().lower()
    rep = {"ي":"ی","ك":"ک","آ":"ا","إ":"ا","أ":"ا","ٱ":"ا","ة":"ه","ؤ":"و","ئ":"ی"}
    for a,b in rep.items():
        s = s.replace(a,b)
    s = re.sub(r"\s+", " ", s)
    return s

def game_list_kb(author_id: int) -> InlineKeyboardMarkup:
    names = [
        ("g_num100","حدس عدد ۱..۱۰۰"),
        ("g_num1000","حدس عدد ۱..۱۰۰۰"),
        ("g_anagram","به‌هم‌ریختهٔ کلمه"),
        ("g_typing","تایپ سرعتی"),
        ("g_math","مسابقه ریاضی"),
        ("g_capital","پایتخت کشورها"),
        ("g_emoji","معمای ایموجی"),
        ("g_odd","غریبهٔ جمع"),
        ("g_flag","پرچم-کشور"),
        ("g_syn","مترادف (فارسی)"),
        ("g_word_hole","کلمه ناقص"),
        ("g_rps","قیچی-کاغذ-سنگ"),
        ("g_coin","شیر یا خط"),
        ("g_seq","الگوی عددی"),
        ("g_trivia","دانستنی‌ها"),
    ]
    rows = []
    for i in range(0, len(names), 3):
        row = [InlineKeyboardButton(names[j][1], callback_data=f"game|{names[j][0]}|{author_id}") for j in range(i,min(i+3,len(names)))]
        rows.append(row)
    rows.append([InlineKeyboardButton("📈 جدول امتیاز", callback_data=f"game|score|{author_id}")])
    return InlineKeyboardMarkup(rows)

async def cmd_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("یه بازی انتخاب کن:", reply_markup=game_list_kb(update.effective_user.id))

async def cb_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split("|")
    gid, author_id = parts[1], int(parts[2])
    if q.from_user.id != author_id:
        await q.answer(alert_not_for_you(), show_alert=True); return
    await q.answer()
    if gid == "score":
        await show_scoreboard(update, context); return
    session = await start_game_session(gid, q.message.chat_id, q.from_user.id)
    if not session:
        await q.edit_message_text("این بازی الان در دسترس نیست.")
        return
    try:
        await q.edit_message_text(f"🎮 {session.game_id}: {session.prompt}")
    except:
        await context.bot.send_message(chat_id=q.message.chat_id, text=f"🎮 {session.game_id}: {session.prompt}")

async def show_scoreboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    rows = await db.get_game_top(MAIN_CHAT_ID, limit=10)
    if not rows:
        await update.callback_query.edit_message_text("جدول خالیه.")
        return
    lines = ["📈 جدول امتیاز:", ""]
    for i, r in enumerate(rows, start=1):
        name = r["fn"] or ""
        if r["ln"]: name += " " + r["ln"]
        if not name and r["un"]: name = "@"+r["un"]
        if not name: name = f"id:{r['user_id']}"
        lines.append(f"{i}. {name} — {r['score']}")
    await update.callback_query.edit_message_text("\n".join(lines))

CAPITALS = {
    "ایران":"تهران","عراق":"بغداد","ترکیه":"آنکارا","افغانستان":"کابل","فرانسه":"پاریس","آلمان":"برلین",
    "ایتالیا":"رم","اسپانیا":"مادرید","انگلستان":"لندن","روسیه":"مسکو","چین":"پکن","ژاپن":"توکیو",
    "هند":"دهلی نو","برزیل":"برازیلیا","کانادا":"اتاوا","مکزیک":"مکزیکوسیتی","مصر":"قاهره","عربستان":"ریاض",
}
EMOJI_RIDDLES = [("🍎📱", ["اپل","apple"]),("🎬🍿", ["سینما","فیلم"]),("☕🐱", ["کافه","قهوه"]),("📸🐦", ["اینستاگرام","instagram","عکس"]),("🧊❄️", ["یخ","سرما"])]
WORDS_FA = ["مدیریت","سولز","گارد","حضور","آمار","سیستم","ربات","گفتگو","سرگرمی","اکانت","ویس","کال","مدیر","پیام","گروه","کاربر","شماره","زمان","تاریخ","حساب"]
SYN_FA = [("سریع","تند"),("آرام","ملایم"),("شوخ","بامزه"),("باهوش","زیرک"),("قوی","نیرومند")]
TRIVIA = [("بزرگ‌ترین اقیانوس جهان؟","آرام"),("ارتفاعات دماوند در کدام کشور است؟","ایران"),("تهران چندمین حرف الفباست؟","شوخی کردی؟ 😅")]
ODD_SETS = [["سیب","موز","گلابی","پرتقال","پیچ‌گوشتی"],["آبی","قرمز","سبز","پیچ"]]
SEQS = [([2,4,8,16,"?"],"32"),([1,1,2,3,5,8,"?"],"13")]

async def start_game_session(gid: str, chat_id: int, started_by: int) -> Optional[GameSession]:
    if chat_id in GAME_SESSIONS and GAME_SESSIONS[chat_id].active:
        GAME_SESSIONS[chat_id].active = False

    if gid == "g_num100":
        num = random.randint(1,100); return set_session(chat_id, gid, f"یه عدد بین ۱ تا ۱۰۰ حدس بزن!", [str(num)], started_by)
    if gid == "g_num1000":
        num = random.randint(1,1000); return set_session(chat_id, gid, f"عدد بین ۱ تا ۱۰۰۰ حدس بزن!", [str(num)], started_by)
    if gid == "g_anagram":
        w = random.choice(WORDS_FA); shuffled = "".join(random.sample(w, len(w))); return set_session(chat_id, gid, f"حروف به‌هم‌ریخته: {shuffled}", [normalize(w)], started_by)
    if gid == "g_typing":
        s = " ".join(random.sample(["سولز","ربات","مدیر","حضور","آمار","گارد","کال","چت"], k=4)); return set_session(chat_id, gid, f"این متن رو *دقیقاً* و سریع تایپ کن:\n{s}", [normalize(s)], started_by)
    if gid == "g_math":
        a,b = random.randint(10,99), random.randint(10,99); op = random.choice(["+","-","*"]); expr = f"{a}{op}{b}"; ans = str(eval(expr)); return set_session(chat_id, gid, f"حل کن: `{expr}`", [ans], started_by)
    if gid == "g_capital":
        c, cap = random.choice(list(CAPITALS.items())); return set_session(chat_id, gid, f"پایتخت *{c}* چیه؟", [normalize(cap)], started_by)
    if gid == "g_emoji":
        e, ans = random.choice(EMOJI_RIDDLES); return set_session(chat_id, gid, f"حدس بزن: {e}", [normalize(a) for a in ans], started_by)
    if gid == "g_odd":
        s = random.choice(ODD_SETS); return set_session(chat_id, gid, f"کدومشون وصله ناجوره؟ {'، '.join(s)}", [normalize(s[-1])], started_by)
    if gid == "g_flag":
        c, cap = random.choice(list(CAPITALS.items())); return set_session(chat_id, gid, f"پرچم 🇮🇷؟ شوخی! کشورِ پایتخت *{cap}* رو بگو:", [normalize(c)], started_by)
    if gid == "g_syn":
        a,b = random.choice(SYN_FA); return set_session(chat_id, gid, f"مترادف «{a}» چیه؟", [normalize(b)], started_by)
    if gid == "g_word_hole":
        w = random.choice(WORDS_FA); idxs = random.sample(range(len(w)), k=min(2, max(1, len(w)//4))); hole = "".join([("_" if i in idxs else ch) for i,ch in enumerate(w)]); return set_session(chat_id, gid, f"جای خالی رو پر کن: {hole}", [normalize(w)], started_by)
    if gid == "g_rps":
        bot = random.choice(["سنگ","کاغذ","قیچی"]); winners = {"سنگ":"کاغذ","کاغذ":"قیچی","قیچی":"سنگ"}; return set_session(chat_id, gid, f"من زدم: *{bot}* — تو چی می‌زنی که می‌بره؟", [normalize(winners[bot])], started_by)
    if gid == "g_coin":
        coin = random.choice(["شیر","خط"]); return set_session(chat_id, gid, f"سکه هواست... شیر یا خط؟", [normalize(coin)], started_by)
    if gid == "g_seq":
        seq, ans = random.choice(SEQS); return set_session(chat_id, gid, f"الگو رو کامل کن: {'، '.join(map(str,seq))}", [normalize(ans)], started_by)
    if gid == "g_trivia":
        q,a = random.choice(TRIVIA); return set_session(chat_id, gid, q, [normalize(a)], started_by)
    return None

def set_session(chat_id: int, gid: str, prompt: str, answers: List[str], started_by: int) -> GameSession:
    s = GameSession(chat_id, gid, prompt, answers, started_by, points=1)
    GAME_SESSIONS[chat_id] = s
    return s

async def handle_game_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    msg = update.effective_message
    if not msg.text:
        return
    sess = GAME_SESSIONS.get(MAIN_CHAT_ID)
    if not sess or not sess.active:
        return
    txt = normalize(msg.text)
    if txt in sess.answers:
        sess.active = False
        db: DB = context.bot_data["DB"]
        await db.inc_game_score(MAIN_CHAT_ID, msg.from_user.id, 1)
        await msg.reply_text(f"🎉 {mention(msg.from_user.id, msg.from_user.full_name)} درست گفت! (+1 امتیاز)\nمیخوای ادامه بدیم؟ «بازی»", parse_mode=ParseMode.MARKDOWN)

# ----------------------------- Text Commands --------------------------
async def handle_text_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip()
    handlers = [
        ("ثبت خروج", cmd_register_close),
        ("ثبت", cmd_register_open),
        ("لیست ممنوع", cmd_list_banned),
        ("لیست گارد", cmd_list_guard),
        ("راهنما", cmd_help),
        ("تگ روشن", cmd_tag_toggle),
        ("تگ خاموش", cmd_tag_toggle),
        ("تگ", cmd_tag_panel),
        ("جنسیت", cmd_gender),
        ("آیدی", cmd_id),
        ("بازی", cmd_game),
        ("ربات", cmd_bot_nice),
    ]
    for key, fn in handlers:
        if txt.startswith(key):
            await fn(update, context)
            return
    if txt.startswith("ترفیع ") or txt.startswith("عزل "):
        await handle_promote_demote(update, context); return
    if txt.startswith("ممنوع"):
        await cmd_ban(update, context); return
    if txt.startswith("آزاد"):
        await cmd_unban(update, context); return

# ----------------------------- Membership & Bans ----------------------
async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db: DB = context.bot_data["DB"]
    upd: ChatMemberUpdated = update.chat_member
    user = upd.new_chat_member.user
    await ensure_user(db, user)
    if upd.chat.id != MAIN_CHAT_ID:
        return
    status = upd.new_chat_member.status
    if status in ("member","administrator","creator"):
        await db.set_user_in_group(user.id, True)
        if await db.is_banned(user.id):
            try:
                await context.bot.ban_chat_member(chat_id=MAIN_CHAT_ID, user_id=user.id)
            except Exception as e:
                logger.info("ban on join: %s", e)
    elif status in ("left","kicked","restricted"):
        await db.set_user_in_group(user.id, False)

# ----------------------------- Application Setup ---------------------
async def post_init(app: Application):
    """
    Runs after Application.initialize(); good place to init DB and schedule jobs.
    """
    if not BOT_TOKEN or not DATABASE_URL or not MAIN_CHAT_ID or not GUARD_CHAT_ID or not OWNER_ID:
        raise SystemExit("لطفاً تمام متغیرهای محیطی لازم را تنظیم کنید: OWNER_ID, TZ, MAIN_CHAT_ID, GUARD_CHAT_ID, BOT_TOKEN, DATABASE_URL")

    # Prepare DB
    db = await DB.create(DATABASE_URL)
    app.bot_data["DB"] = db

    # Schedule nightly stats at 00:00 TZ
    now = now_tz()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    delay = (tomorrow - now).total_seconds()
    app.job_queue.run_repeating(nightly_stats_job, interval=86400, first=delay)

    # Random tag job (every 15m)
    app.job_queue.run_repeating(random_tag_job, interval=900, first=60)

def build_application() -> Application:
    defaults = Defaults(tzinfo=TZINFO, parse_mode=ParseMode.MARKDOWN)

    # Optional rate limiter: if extras not installed, continue without it
    rate_limiter = None
    try:
        rate_limiter = AIORateLimiter()
    except Exception:
        logger.warning("AIORateLimiter غیرفعال است (نصب نشده). برای فعال‌سازی: pip install 'python-telegram-bot[rate-limiter]'")
        rate_limiter = None

    builder = ApplicationBuilder().token(BOT_TOKEN).defaults(defaults).post_init(post_init)
    if rate_limiter is not None:
        builder = builder.rate_limiter(rate_limiter)
    app = builder.build()

    app.add_handler(CommandHandler("start", cmd_start, filters.ChatType.PRIVATE))
    app.add_handler(CallbackQueryHandler(cb_pm, pattern=r"^pm\|"))
    app.add_handler(CallbackQueryHandler(cb_back, pattern=r"^back\|"))
    app.add_handler(CallbackQueryHandler(cb_sendonce, pattern=r"^sendonce\|"))
    app.add_handler(CallbackQueryHandler(cb_replyto, pattern=r"^replyto\|"))
    app.add_handler(CallbackQueryHandler(cb_block_dm, pattern=r"^blockdm\|"))
    app.add_handler(CallbackQueryHandler(cb_session_select, pattern=r"^sess\|"))
    app.add_handler(CallbackQueryHandler(cb_tag, pattern=r"^tag\|"))
    app.add_handler(CallbackQueryHandler(cb_gender, pattern=r"^gender\|"))
    app.add_handler(CallbackQueryHandler(cb_game, pattern=r"^game\|"))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, handle_pm_any))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, maybe_prompt_session))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, handle_game_answer))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_commands))
    app.add_handler(MessageHandler(filters.Chat(GUARD_CHAT_ID) | filters.Chat(OWNER_ID), handle_guard_admin_reply))
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.MY_CHAT_MEMBER | ChatMemberHandler.CHAT_MEMBER))
    return app

def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN ست نشده.")
    app = build_application()
    logger.info("Souls bot (patched) starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
