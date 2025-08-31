
# -*- coding: utf-8 -*-
"""
Souls / Souls Guard Telegram Bot (single-file)
- Framework: aiogram v3
- DB: PostgreSQL via asyncpg
- Hosting: Railway
- Env Vars:
  BOT_TOKEN, DATABASE_URL, MAIN_CHAT_ID, GUARD_CHAT_ID, OWNER_ID, TZ

New in this version:
- پشتیبانی @username در دستورات (با lookup از دیتابیس)
- «لیست‌های سفارشی» برای افزودن/حذف/نمایش/تگ اعضا از طریق دستورات
- حداقل ۱۰ بازی: حدس عدد x2، تایپ سرعتی، سنگ‌کاغذقیچی، ریاضی سریع، حدس کلمه، تکمیل کلمه،
  درهم‌ریخته (anagram)، درست/نادرست، حافظه‌ی عددی
"""
import os
import asyncio
import re
import random
import string
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Optional, Tuple, List, Any

import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
                           ChatMemberUpdated)
from aiogram.filters import CommandStart
from aiogram.enums import ChatType, ParseMode
from aiogram.client.default import DefaultBotProperties

# Persian (Jalali) date
try:
    import jdatetime
except Exception:
    jdatetime = None

# ---------- ENV & GLOBALS ----------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
MAIN_CHAT_ID = int(os.getenv("MAIN_CHAT_ID", "0"))
GUARD_CHAT_ID = int(os.getenv("GUARD_CHAT_ID", "0"))
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
TZ = os.getenv("TZ", "Asia/Tehran")

if not BOT_TOKEN or not DATABASE_URL or not MAIN_CHAT_ID or not GUARD_CHAT_ID or not OWNER_ID:
    raise SystemExit("Please set BOT_TOKEN, DATABASE_URL, MAIN_CHAT_ID, GUARD_CHAT_ID, OWNER_ID")

ADMIN_REPLY_STATE: Dict[int, int] = {}
USER_ROUTE_STATE: Dict[int, str] = {}
SESSION_IDLE_TASKS: Dict[int, asyncio.Task] = {}
GAME_STATE: Dict[int, Dict[str, Any]] = {}

ROLE_ORDER = ["owner","lead_all","lead_chat","lead_call","admin_chat","admin_call"]
MENTION_CHUNK = 5

# ---------- UTILS ----------
def now_tehran() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)

def fmt_jalali(d: date) -> str:
    try:
        if jdatetime:
            j = jdatetime.date.fromgregorian(date=d)
            weekdays = ["دوشنبه","سه‌شنبه","چهارشنبه","پنجشنبه","جمعه","شنبه","یکشنبه"]
            return f"{j.strftime('%Y/%m/%d')} - {weekdays[j.weekday() % 7]}"
    except Exception:
        pass
    return d.isoformat()

def mention_html(user_id: int, name: str) -> str:
    safe = (name or "کاربر").replace("<","").replace(">","")
    return f"<a href=\"tg://user?id={user_id}\">{safe}</a>"

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ---------- DB LAYER ----------
class DB:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=10)
        await self.init_schema()

    async def init_schema(self):
        async with self.pool.acquire() as con:
            await con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                username_lc TEXT,
                gender TEXT,
                is_banned BOOLEAN DEFAULT FALSE,
                last_active TIMESTAMP WITH TIME ZONE,
                joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            CREATE UNIQUE INDEX IF NOT EXISTS users_username_lc_idx ON users(username_lc) WHERE username_lc IS NOT NULL;

            CREATE TABLE IF NOT EXISTS roles (
                user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                role TEXT
            );

            CREATE TABLE IF NOT EXISTS bans (
                user_id BIGINT PRIMARY KEY,
                reason TEXT,
                banned_by BIGINT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                kind TEXT NOT NULL, -- chat|call
                start_at TIMESTAMP WITH TIME ZONE NOT NULL,
                end_at TIMESTAMP WITH TIME ZONE,
                last_msg_at TIMESTAMP WITH TIME ZONE
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_open ON sessions(user_id) WHERE end_at IS NULL;

            CREATE TABLE IF NOT EXISTS daily_stats (
                day DATE NOT NULL,
                user_id BIGINT NOT NULL,
                chat_msgs INT DEFAULT 0,
                mentions INT DEFAULT 0,
                call_minutes INT DEFAULT 0,
                PRIMARY KEY (day, user_id)
            );

            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            -- Named lists for full DB control
            CREATE TABLE IF NOT EXISTS named_lists (
                name TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS list_members (
                name TEXT REFERENCES named_lists(name) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                PRIMARY KEY (name, user_id)
            );
            """)

    async def upsert_user(self, u):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO users (user_id, first_name, last_name, username, username_lc, last_active)
                VALUES ($1,$2,$3,$4,LOWER($4),NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    first_name=EXCLUDED.first_name,
                    last_name=EXCLUDED.last_name,
                    username=EXCLUDED.username,
                    username_lc=EXCLUDED.username_lc,
                    last_active=NOW();
            """, u.id, u.first_name, u.last_name, u.username)

    async def ensure_user_id(self, user_id: int):
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id)

    async def get_user_id_by_username(self, username: str) -> Optional[int]:
        async with self.pool.acquire() as con:
            return await con.fetchval("SELECT user_id FROM users WHERE username_lc=$1", username.lower())

    async def set_gender(self, user_id: int, gender: str):
        async with self.pool.acquire() as con:
            await con.execute("UPDATE users SET gender=$2 WHERE user_id=$1", user_id, gender)

    async def get_role(self, user_id: int) -> Optional[str]:
        async with self.pool.acquire() as con:
            return await con.fetchval("SELECT role FROM roles WHERE user_id=$1", user_id)

    async def set_role(self, user_id: int, role: Optional[str]):
        async with self.pool.acquire() as con:
            if role:
                await con.execute("""
                    INSERT INTO roles (user_id, role) VALUES ($1,$2)
                    ON CONFLICT (user_id) DO UPDATE SET role=EXCLUDED.role
                """, user_id, role)
            else:
                await con.execute("DELETE FROM roles WHERE user_id=$1", user_id)

    async def list_by_roles(self, roles: List[str]) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT u.user_id, u.first_name, u.last_name, u.username, r.role
                FROM roles r JOIN users u ON u.user_id=r.user_id
                WHERE r.role = ANY($1::text[])
                ORDER BY ARRAY_POSITION($1::text[], r.role), u.first_name
            """, roles)
            return list(rows)

    async def record_message(self, user_id: int, is_mention: bool):
        async with self.pool.acquire() as con:
            await con.execute("UPDATE users SET last_active=NOW() WHERE user_id=$1", user_id)
            await con.execute("""
                INSERT INTO daily_stats (day, user_id, chat_msgs, mentions)
                VALUES (CURRENT_DATE, $1, 1, $2)
                ON CONFLICT (day, user_id) DO UPDATE SET
                    chat_msgs = daily_stats.chat_msgs + 1,
                    mentions = daily_stats.mentions + $2
            """, user_id, 1 if is_mention else 0)

    async def open_session(self, user_id: int, kind: str):
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO sessions (user_id, kind, start_at, last_msg_at) VALUES ($1,$2,NOW(),NOW())", user_id, kind)

    async def has_open_session(self, user_id: int) -> bool:
        async with self.pool.acquire() as con:
            v = await con.fetchval("SELECT 1 FROM sessions WHERE user_id=$1 AND end_at IS NULL", user_id)
            return bool(v)

    async def touch_session(self, user_id: int):
        async with self.pool.acquire() as con:
            await con.execute("UPDATE sessions SET last_msg_at=NOW() WHERE user_id=$1 AND end_at IS NULL", user_id)

    async def close_open_session(self, user_id: int):
        async with self.pool.acquire() as con:
            await con.execute("UPDATE sessions SET end_at=NOW() WHERE user_id=$1 AND end_at IS NULL", user_id)

    async def auto_close_idle_sessions(self, idle_minutes: int = 5) -> List[int]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                UPDATE sessions s SET end_at=NOW()
                WHERE s.end_at IS NULL AND s.last_msg_at < NOW() - ($1 || ' minutes')::interval
                RETURNING s.user_id
            """, idle_minutes)
            return [r["user_id"] for r in rows]

    async def sum_call_minutes_for_day(self, d: date) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT user_id,
                       SUM(EXTRACT(EPOCH FROM (end_at - start_at))/60) AS mins
                FROM sessions
                WHERE end_at IS NOT NULL AND DATE(end_at AT TIME ZONE 'UTC')=$1 AND kind='call'
                GROUP BY user_id
            """, d)
            return list(rows)

    async def upsert_call_minutes(self, d: date, user_id: int, minutes: int):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO daily_stats (day, user_id, call_minutes) VALUES ($1,$2,$3)
                ON CONFLICT (day, user_id) DO UPDATE SET call_minutes=$3
            """, d, user_id, minutes)

    async def get_stats_last7(self, user_id: int) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT day, chat_msgs, mentions, call_minutes
                FROM daily_stats
                WHERE user_id=$1 AND day >= CURRENT_DATE - INTERVAL '6 days'
                ORDER BY day
            """, user_id)
            return list(rows)

    async def list_active_members(self, minutes: int = 120) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT user_id, first_name, last_name, username
                FROM users
                WHERE last_active > NOW() - ($1 || ' minutes')::interval AND COALESCE(is_banned,false)=false
            """, minutes)
            return list(rows)

    async def by_gender(self, gender: str) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("SELECT user_id, first_name, last_name FROM users WHERE gender=$1", gender)
            return list(rows)

    async def add_ban(self, target_id: int, by: int, reason: str = None):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO bans (user_id, reason, banned_by) VALUES ($1,$2,$3)
                ON CONFLICT (user_id) DO UPDATE SET reason=EXCLUDED.reason, banned_by=EXCLUDED.banned_by, created_at=NOW();
                UPDATE users SET is_banned=TRUE WHERE user_id=$1;
            """, target_id, reason, by)

    async def remove_ban(self, target_id: int):
        async with self.pool.acquire() as con:
            await con.execute("""
                DELETE FROM bans WHERE user_id=$1;
                UPDATE users SET is_banned=FALSE WHERE user_id=$1;
            """, target_id)

    async def list_bans(self) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT b.user_id, u.first_name, u.last_name FROM bans b LEFT JOIN users u ON u.user_id=b.user_id
                ORDER BY b.created_at DESC
            """)
            return list(rows)

    # named lists
    async def create_list(self, name: str):
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO named_lists (name) VALUES ($1) ON CONFLICT DO NOTHING", name)

    async def delete_list(self, name: str):
        async with self.pool.acquire() as con:
            await con.execute("DELETE FROM named_lists WHERE name=$1", name)

    async def add_to_list(self, name: str, user_id: int):
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO named_lists (name) VALUES ($1) ON CONFLICT DO NOTHING", name)
            await con.execute("INSERT INTO list_members (name, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING", name, user_id)

    async def remove_from_list(self, name: str, user_id: int):
        async with self.pool.acquire() as con:
            await con.execute("DELETE FROM list_members WHERE name=$1 AND user_id=$2", name, user_id)

    async def members_of_list(self, name: str) -> List[asyncpg.Record]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT u.user_id, u.first_name, u.last_name
                FROM list_members m LEFT JOIN users u ON u.user_id=m.user_id
                WHERE m.name=$1
            """, name)
            return list(rows)

    async def set_config(self, key: str, val: str):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO config (key, value) VALUES ($1,$2)
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
            """, key, val)

    async def get_config(self, key: str, default: str = "") -> str:
        async with self.pool.acquire() as con:
            v = await con.fetchval("SELECT value FROM config WHERE key=$1", key)
            return v if v is not None else default

# ---------- BOT ----------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
r_priv = Router()
r_grp = Router()
dp.include_router(r_priv)
dp.include_router(r_grp)

db = DB(DATABASE_URL)

# ---------- START / PRIVATE PANEL ----------
def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📨 تماس با گارد مدیران", callback_data="pm_guard")],
        [InlineKeyboardButton(text="👑 ارتباط با مالک", callback_data="pm_owner")],
        [InlineKeyboardButton(text="📊 آمار من", callback_data="my_stats")]
    ])

@r_priv.message(CommandStart(), F.chat.type==ChatType.PRIVATE)
async def start_cmd(msg: Message):
    await db.upsert_user(msg.from_user)
    await msg.answer("سلام! به ربات سولز خوش اومدی.\nاز دکمه‌های زیر استفاده کن 👇", reply_markup=main_menu_kb())

@r_priv.callback_query(F.data.in_(["pm_guard","pm_owner"]))
async def pm_route(cb: CallbackQuery):
    USER_ROUTE_STATE[cb.from_user.id] = "guard" if cb.data=="pm_guard" else "owner"
    await db.upsert_user(cb.from_user)
    await cb.message.answer("پیامت رو بفرست؛ هر نوع پیامی مجازه. بعد از ارسال، می‌تونی «ارسال مجدد» بزنی.")
    await cb.answer()

def resend_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ ارسال مجدد", callback_data="resend")],
                                                 [InlineKeyboardButton(text="⬅️ بازگشت", callback_data="back_menu")]])

def reply_block_kb(target_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✉️ پاسخ", callback_data=f"reply:{target_id}"),
        InlineKeyboardButton(text="⛔ مسدود", callback_data=f"block:{target_id}")
    ]])

@r_priv.callback_query(F.data=="back_menu")
async def back_menu(cb: CallbackQuery):
    await cb.message.edit_text("منو:", reply_markup=main_menu_kb())
    await cb.answer()

@r_priv.callback_query(F.data=="resend")
async def resend(cb: CallbackQuery):
    await cb.message.answer("پیامت رو دوباره بفرست.")
    await cb.answer()

@r_priv.message(F.chat.type==ChatType.PRIVATE)
async def private_inbox(msg: Message):
    route = USER_ROUTE_STATE.get(msg.from_user.id)
    await db.upsert_user(msg.from_user)
    if route not in ("guard","owner"):
        return
    target_chat = GUARD_CHAT_ID if route=="guard" else OWNER_ID
    try:
        await msg.copy_to(target_chat)
        caption = f"پیام جدید از {mention_html(msg.from_user.id, msg.from_user.full_name)}"
        await bot.send_message(target_chat, caption, reply_markup=reply_block_kb(msg.from_user.id))
    except Exception:
        await msg.answer("ارسال پیام به مقصد ناموفق بود.")
        return
    await msg.answer("پیام ارسال شد ✅", reply_markup=resend_kb())

# ---------- ADMIN REPLY ----------
@r_grp.callback_query(F.data.startswith("reply:"))
async def reply_btn(cb: CallbackQuery):
    ADMIN_REPLY_STATE[cb.from_user.id] = int(cb.data.split(":")[1])
    await db.upsert_user(cb.from_user)
    await cb.message.reply("پیامت رو برای کاربر ارسال کن. (فقط اولین پیام ارسال می‌شود)",
                           reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="لغو", callback_data="cancel_reply")]]))
    await cb.answer()

@r_grp.callback_query(F.data=="cancel_reply")
async def cancel_reply(cb: CallbackQuery):
    ADMIN_REPLY_STATE.pop(cb.from_user.id, None)
    await cb.message.edit_text("لغو شد.")
    await cb.answer()

@r_grp.callback_query(F.data.startswith("block:"))
async def block_btn(cb: CallbackQuery):
    target_id = int(cb.data.split(":")[1])
    role = await db.get_role(cb.from_user.id)
    if cb.from_user.id!=OWNER_ID and role not in ("lead_all","lead_chat","lead_call","admin_chat","admin_call"):
        await cb.answer("اجازه ندارید.", show_alert=True); return
    await db.add_ban(target_id, cb.from_user.id, reason="panel_block")
    for cid in (MAIN_CHAT_ID, GUARD_CHAT_ID):
        try: await bot.ban_chat_member(cid, target_id)
        except Exception: pass
    await cb.message.reply(f"کاربر {target_id} مسدود شد.")
    await cb.answer("مسدود شد.")

@r_grp.message(F.chat.id==GUARD_CHAT_ID)
async def admin_reply_pipe(msg: Message):
    target = ADMIN_REPLY_STATE.pop(msg.from_user.id, None)
    if target:
        try:
            await msg.copy_to(target)
            await msg.reply("ارسال شد ✅", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="پاسخ مجدد", callback_data=f"reply:{target}")]]))
            await bot.send_message(target, "پاسخ مدیران رسید ✅", reply_markup=main_menu_kb())
        except Exception:
            await msg.reply("خطا در ارسال پاسخ.")

# ---------- PARSE TARGET (reply / id / @username) ----------
async def parse_target_from_message(msg: Message) -> Optional[int]:
    if msg.reply_to_message and msg.reply_to_message.from_user:
        return msg.reply_to_message.from_user.id
    text = (msg.text or msg.caption or "").strip()
    # numeric id
    m = re.search(r'(\d{6,})', text)
    if m:
        try: return int(m.group(1))
        except: pass
    # @username
    um = re.search(r'@([A-Za-z0-9_]{5,})', text)
    if um:
        uid = await db.get_user_id_by_username(um.group(1))
        if uid: return uid
    return None

# ---------- GROUP MONITOR (main) ----------
@r_grp.message(F.chat.id==MAIN_CHAT_ID)
async def main_group_listener(msg: Message):
    if not msg.from_user or msg.from_user.is_bot: return
    await db.upsert_user(msg.from_user)
    # stats
    is_mention = False
    if msg.entities:
        for e in msg.entities:
            if e.type in ("mention","text_mention"):
                is_mention = True; break
    await db.record_message(msg.from_user.id, is_mention)

    role = await db.get_role(msg.from_user.id)
    if role in ("admin_chat","admin_call","lead_chat","lead_call","lead_all") or msg.from_user.id==OWNER_ID:
        if not await db.has_open_session(msg.from_user.id):
            await msg.reply("مدیر عزیز، نوع فعالیتت رو ثبت کن:", reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🟢 ثبت فعالیت چت", callback_data="session:chat")],
                                 [InlineKeyboardButton(text="🔵 ثبت فعالیت کال", callback_data="session:call")]]))
        else:
            await db.touch_session(msg.from_user.id)
            t = SESSION_IDLE_TASKS.get(msg.from_user.id)
            if t and not t.cancelled(): t.cancel()
            SESSION_IDLE_TASKS[msg.from_user.id] = asyncio.create_task(schedule_idle_close(msg.from_user.id, 5))

async def schedule_idle_close(user_id: int, minutes: int):
    try: await asyncio.sleep(minutes*60)
    except asyncio.CancelledError: return
    if await db.has_open_session(user_id):
        await db.close_open_session(user_id)
        try: await bot.send_message(GUARD_CHAT_ID, f"🔴 پایان خودکار فعالیت {user_id} به دلیل عدم فعالیت ۵ دقیقه‌ای")
        except Exception: pass

@r_grp.callback_query(F.data.startswith("session:"))
async def session_buttons(cb: CallbackQuery):
    kind = cb.data.split(":")[1]
    role = await db.get_role(cb.from_user.id)
    if role not in ("admin_chat","admin_call","lead_chat","lead_call","lead_all") and cb.from_user.id!=OWNER_ID:
        await cb.answer("فقط مدیران می‌توانند ثبت کنند.", show_alert=True); return
    if await db.has_open_session(cb.from_user.id):
        await cb.answer("سشن باز دارید.", show_alert=True); return
    await db.open_session(cb.from_user.id, kind)
    await cb.message.reply(f"✅ شروع فعالیت {mention_html(cb.from_user.id, cb.from_user.full_name)} ({'چت' if kind=='chat' else 'کال'})")
    try: await bot.send_message(GUARD_CHAT_ID, f"✅ شروع فعالیت {mention_html(cb.from_user.id, cb.from_user.full_name)} - نوع: {'چت' if kind=='chat' else 'کال'}")
    except Exception: pass
    SESSION_IDLE_TASKS[cb.from_user.id] = asyncio.create_task(schedule_idle_close(cb.from_user.id, 5))
    await cb.answer("ثبت شد.")

@r_grp.message(F.text.regexp("^ثبت خروج$"), F.chat.id==MAIN_CHAT_ID)
async def end_session_text(msg: Message):
    if await db.has_open_session(msg.from_user.id):
        await db.close_open_session(msg.from_user.id)
        await msg.reply("پایان فعالیت شما گزارش شد. خسته نباشید 🤍")
        try: await bot.send_message(GUARD_CHAT_ID, f"🔻 پایان فعالیت {mention_html(msg.from_user.id, msg.from_user.full_name)}")
        except Exception: pass
    else:
        await msg.reply("سشن بازی ندارید.")

@r_grp.message(F.text.regexp("^ثبت$"), F.chat.id==MAIN_CHAT_ID)
async def popup_register(msg: Message):
    await msg.reply("نوع فعالیت را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🟢 ثبت فعالیت چت", callback_data="session:chat")],
                         [InlineKeyboardButton(text="🔵 ثبت فعالیت کال", callback_data="session:call")]]))

# ---------- MANAGEMENT ----------
def only_privileged(uid: int, role: Optional[str]) -> bool:
    return uid==OWNER_ID or (role in ("lead_all","lead_chat","lead_call","admin_chat","admin_call"))

@r_grp.message(F.text.regexp("^ممنوع"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def ban_cmd(msg: Message):
    role = await db.get_role(msg.from_user.id)
    if not only_privileged(msg.from_user.id, role): return
    target = await parse_target_from_message(msg)
    if not target: await msg.reply("ریپلای/آیدی/@username لازم است."); return
    await db.add_ban(target, msg.from_user.id, reason="manual_ban")
    for cid in (MAIN_CHAT_ID, GUARD_CHAT_ID):
        try: await bot.ban_chat_member(cid, target)
        except Exception: pass
    await msg.reply(f"کاربر {target} ممنوع شد.")

@r_grp.message(F.text.regexp("^آزاد"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def unban_cmd(msg: Message):
    role = await db.get_role(msg.from_user.id)
    if not only_privileged(msg.from_user.id, role): return
    target = await parse_target_from_message(msg)
    if not target: await msg.reply("ریپلای/آیدی/@username لازم است."); return
    await db.remove_ban(target)
    for cid in (MAIN_CHAT_ID, GUARD_CHAT_ID):
        try: await bot.unban_chat_member(cid, target, only_if_banned=True)
        except Exception: pass
    await msg.reply(f"کاربر {target} آزاد شد.")

@r_grp.message(F.text.regexp("^لیست ممنوع"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def list_banned(msg: Message):
    rows = await db.list_bans()
    if not rows: await msg.reply("لیست ممنوع خالی است."); return
    lines = [f"• {mention_html(r['user_id'], ((r['first_name'] or '') + ' ' + (r['last_name'] or '')).strip() or str(r['user_id']))}" for r in rows]
    await msg.reply("\n".join(lines), disable_web_page_preview=True)

PROMO_MAP = {
    "ترفیع چت": "admin_chat",
    "ترفیع کال": "admin_call",
    "ترفیع ارشد چت": "lead_chat",
    "ترفیع ارشد کال": "lead_call",
    "ترفیع ارشد کل": "lead_all",
}
DEMOTE_KEYS = ["عزل چت","عزل کال","عزل ارشد چت","عزل ارشد کال","عزل ارشد کل"]

@r_grp.message(F.text.func(lambda t: any(t.startswith(k) for k in PROMO_MAP.keys())), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def promote_cmd(msg: Message):
    if msg.from_user.id != OWNER_ID: return
    target = await parse_target_from_message(msg)
    if not target: await msg.reply("ریپلای/آیدی/@username لازم است."); return
    role = next(v for k,v in PROMO_MAP.items() if msg.text.startswith(k))
    await db.ensure_user_id(target)
    await db.set_role(target, role)
    await msg.reply(f"سمت {target} → {role} ثبت شد.")

@r_grp.message(F.text.func(lambda t: any(t.startswith(k) for k in DEMOTE_KEYS)), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def demote_cmd(msg: Message):
    if msg.from_user.id != OWNER_ID: return
    target = await parse_target_from_message(msg)
    if not target: await msg.reply("ریپلای/آیدی/@username لازم است."); return
    await db.set_role(target, None)
    await msg.reply(f"سمت {target} حذف شد.")

@r_grp.message(F.text.regexp("^لیست گارد"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def list_guard(msg: Message):
    rows = await db.list_by_roles(ROLE_ORDER)
    if not rows: await msg.reply("لیستی موجود نیست."); return
    lines = []
    for r in rows:
        name = ((r['first_name'] or '') + ' ' + (r['last_name'] or '')).strip() or str(r['user_id'])
        lines.append(f"• {r['role']}: {mention_html(r['user_id'], name)}")
    await msg.reply("\n".join(lines), disable_web_page_preview=True)

@r_grp.message(F.text.regexp("^آیدی"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def id_stats(msg: Message):
    target = await parse_target_from_message(msg) or msg.from_user.id
    rows = await db.get_stats_last7(target)
    if not rows: await msg.reply("آماری موجود نیست."); return
    lines = [f"آمار ۷ روز گذشته {target}"]
    for r in rows:
        d = r["day"]; d = d if isinstance(d, date) else r["day"].date()
        lines.append(f"{fmt_jalali(d)}: پیام‌ها {r['chat_msgs']} | منشن {r['mentions']} | کال {int(r['call_minutes'])} دقیقه")
    try:
        photos = await bot.get_user_profile_photos(target, limit=1)
        if photos.total_count>0:
            fid = photos.photos[0][-1].file_id
            await bot.send_photo(msg.chat.id, fid, caption="\n".join(lines)); return
    except Exception: pass
    await msg.reply("\n".join(lines))

# ---------- TAG PANEL & GENDER ----------
def tag_panel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📣 تگ کال", callback_data="tag:call"),
         InlineKeyboardButton(text="💬 تگ چت", callback_data="tag:chat")],
        [InlineKeyboardButton(text="🔥 تگ اعضای فعال", callback_data="tag:active")],
        [InlineKeyboardButton(text="👧 تگ دخترها", callback_data="tag:girls"),
         InlineKeyboardButton(text="👦 تگ پسرها", callback_data="tag:boys")],
        [InlineKeyboardButton(text="🗂 تگ لیست سفارشی", callback_data="tag:list")]
    ])

@r_grp.message(F.text.regexp("^تگ$"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def tag_cmd(msg: Message):
    await msg.reply("یکی را انتخاب کن:", reply_markup=tag_panel_kb())

@r_grp.callback_query(F.data.startswith("tag:"))
async def tag_actions(cb: CallbackQuery):
    t = cb.data.split(":")[1]
    if t in ("call","chat"):
        roles = ["admin_call","lead_call","lead_all","owner"] if t=="call" else ["admin_chat","lead_chat","lead_all","owner"]
        rows = await db.list_by_roles(roles)
        mentions = [mention_html(r["user_id"], ((r["first_name"] or "") + " " + (r["last_name"] or "")).strip()) for r in rows]
        await send_mentions(cb.message, mentions)
    elif t=="active":
        rows = await db.list_active_members(240)
        mentions = [mention_html(r["user_id"], ((r["first_name"] or "") + " " + (r["last_name"] or "")).strip()) for r in rows]
        await send_mentions(cb.message, mentions)
    elif t=="girls":
        rows = await db.by_gender("girl")
        mentions = [mention_html(r["user_id"], ((r["first_name"] or "") + " " + (r["last_name"] or "")).strip()) for r in rows]
        await send_mentions(cb.message, mentions)
    elif t=="boys":
        rows = await db.by_gender("boy")
        mentions = [mention_html(r["user_id"], ((r["first_name"] or "") + " " + (r["last_name"] or "")).strip()) for r in rows]
        await send_mentions(cb.message, mentions)
    elif t=="list":
        await cb.message.reply("نام لیست را با دستور «تگ لیست <name>» بفرستید.")
    await cb.answer()

async def send_mentions(message: Message, mentions: List[str]):
    if not mentions:
        await message.reply("کسی یافت نشد."); return
    for g in chunks(mentions, MENTION_CHUNK):
        await asyncio.sleep(0.5); await message.reply(" ".join(g), disable_web_page_preview=True)

def gender_kb(target_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="👦 پسر", callback_data=f"gender:boy:{target_id}"),
        InlineKeyboardButton(text="👧 دختر", callback_data=f"gender:girl:{target_id}")
    ]])

@r_grp.message(F.text.regexp("^جنسیت"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def gender_cmd(msg: Message):
    role = await db.get_role(msg.from_user.id)
    if not only_privileged(msg.from_user.id, role): return
    target = await parse_target_from_message(msg) or msg.from_user.id
    await msg.reply("جنسیت را انتخاب کنید:", reply_markup=gender_kb(target))

@r_grp.callback_query(F.data.startswith("gender:"))
async def gender_set(cb: CallbackQuery):
    _, g, tid = cb.data.split(":"); tid = int(tid)
    await db.set_gender(tid, "boy" if g=="boy" else "girl")
    await cb.message.reply(f"جنسیت کاربر {tid} ثبت شد."); await cb.answer("OK")

# ---------- CUSTOM LISTS (full DB control) ----------
def parse_after_keyword(text: str, key: str) -> str:
    return text[len(key):].strip()

@r_grp.message(F.text.regexp("^لیست ساخت "), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def list_create(msg: Message):
    if msg.from_user.id!=OWNER_ID: return
    name = parse_after_keyword(msg.text, "لیست ساخت")
    if not name: await msg.reply("نام لیست؟"); return
    await db.create_list(name); await msg.reply(f"لیست «{name}» ساخته شد.")

@r_grp.message(F.text.regexp("^لیست حذف "), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def list_delete(msg: Message):
    if msg.from_user.id!=OWNER_ID: return
    name = parse_after_keyword(msg.text, "لیست حذف")
    if not name: await msg.reply("نام لیست؟"); return
    await db.delete_list(name); await msg.reply(f"لیست «{name}» حذف شد.")

@r_grp.message(F.text.regexp("^لیست افزودن "), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def list_add_member(msg: Message):
    role = await db.get_role(msg.from_user.id)
    if not only_privileged(msg.from_user.id, role): return
    name = parse_after_keyword(msg.text, "لیست افزودن")
    target = await parse_target_from_message(msg)
    if not name or not target: await msg.reply("فرمت: «لیست افزودن <name>» + ریپلای/آیدی/@username"); return
    await db.ensure_user_id(target); await db.add_to_list(name, target)
    await msg.reply(f"به «{name}» اضافه شد: {target}")

@r_grp.message(F.text.regexp("^لیست کم "), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def list_remove_member(msg: Message):
    role = await db.get_role(msg.from_user.id)
    if not only_privileged(msg.from_user.id, role): return
    name = parse_after_keyword(msg.text, "لیست کم")
    target = await parse_target_from_message(msg)
    if not name or not target: await msg.reply("فرمت: «لیست کم <name>» + ریپلای/آیدی/@username"); return
    await db.remove_from_list(name, target); await msg.reply(f"از «{name}» حذف شد: {target}")

@r_grp.message(F.text.regexp("^لیست نشان "), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def list_show(msg: Message):
    name = parse_after_keyword(msg.text, "لیست نشان")
    rows = await db.members_of_list(name)
    if not rows: await msg.reply("لیست خالی/ناموجود."); return
    mentions = [mention_html(r["user_id"], ((r["first_name"] or "") + " " + (r["last_name"] or "")).strip() or str(r["user_id"])) for r in rows]
    await msg.reply("\n".join(mentions), disable_web_page_preview=True)

@r_grp.message(F.text.regexp("^تگ لیست "), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def list_tag(msg: Message):
    name = parse_after_keyword(msg.text, "تگ لیست")
    rows = await db.members_of_list(name)
    mentions = [mention_html(r["user_id"], ((r["first_name"] or "") + " " + (r["last_name"] or "")).strip() or str(r["user_id"])) for r in rows]
    await send_mentions(msg, mentions)

# ---------- HELP ----------
HELP_TEXT = (
"راهنمای ربات:\n"
"• ثبت حضور: «ثبت» یا دکمه‌ها. پایان: «ثبت خروج».\n"
"• مدیریت: «ممنوع/آزاد»، ترفیع/عزل، «لیست گارد»، «لیست ممنوع»، «جنسیت».\n"
"• لیست‌های سفارشی: «لیست ساخت <name>»، «لیست حذف <name>»، «لیست افزودن <name>»، «لیست کم <name>»، «لیست نشان <name>»، «تگ لیست <name>».\n"
"• آمار: «آیدی» (خود/هدف) – ۷ روز گذشته.\n"
"• تگ: پنل «تگ».\n"
"• بازی: «بازی» و یکی از آیتم‌ها.\n"
)

@r_grp.message(F.text.regexp("^راهنما"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def help_cmd(msg: Message):
    await msg.reply(HELP_TEXT)

# ---------- FUN (random tag toggle) ----------
RANDOM_TAG_PHRASES = [f"جمله انگیزشی {i}" for i in range(1,201)] + [
"سولز بدون تو یه چیزیش کمه!", "یک سلام گرم بده 😄", "بیا که کار داریم!", "وقت درخشیدنه ✨"
]

async def random_tag_loop():
    while True:
        try:
            enabled = (await db.get_config("random_tag_enabled","0"))=="1"
            if enabled:
                # کسی که در 24 ساعت اخیر فعال بوده ولی 60 دقیقه اخیر نبوده
                async with db.pool.acquire() as con:
                    rows = await con.fetch("SELECT user_id, last_active FROM users WHERE last_active IS NOT NULL")
                now = now_tehran()
                candidates = [r["user_id"] for r in rows if (now - r["last_active"]).total_seconds() > 3600]
                if candidates:
                    uid = random.choice(candidates)
                    try:
                        chat = await bot.get_chat(uid)
                        await bot.send_message(MAIN_CHAT_ID, f"{mention_html(uid, chat.full_name)} — {random.choice(RANDOM_TAG_PHRASES)}")
                    except Exception: pass
            await asyncio.sleep(900)
        except Exception:
            await asyncio.sleep(60)

@r_grp.message(F.text.regexp("^تگ روشن$"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def tag_on(msg: Message):
    if msg.from_user.id!=OWNER_ID: return
    await db.set_config("random_tag_enabled","1"); await msg.reply("تگ تصادفی روشن شد ✅")

@r_grp.message(F.text.regexp("^تگ خاموش$"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def tag_off(msg: Message):
    if msg.from_user.id!=OWNER_ID: return
    await db.set_config("random_tag_enabled","0"); await msg.reply("تگ تصادفی خاموش شد ⛔️")

# ---------- PRIVATE: MY STATS ----------
@r_priv.callback_query(F.data=="my_stats")
async def my_stats(cb: CallbackQuery):
    rows = await db.get_stats_last7(cb.from_user.id)
    if not rows:
        await cb.message.answer("آماری یافت نشد."); await cb.answer(); return
    lines = [f"آمار ۷ روز گذشته {mention_html(cb.from_user.id, cb.from_user.full_name)}"]
    for r in rows:
        d = r["day"]; d = d if isinstance(d, date) else r["day"].date()
        lines.append(f"{fmt_jalali(d)}: پیام‌ها {r['chat_msgs']} | منشن {r['mentions']} | کال {int(r['call_minutes'])} دقیقه")
    await cb.message.answer("\n".join(lines), disable_web_page_preview=True); await cb.answer()

# ---------- DAILY AGG ----------
async def daily_aggregation_loop():
    while True:
        try:
            now = now_tehran()
            tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
            delta = max(60.0, (tomorrow - now).total_seconds())
            await asyncio.sleep(delta)
            d = (now + timedelta(hours=3, minutes=30)).date()
            rows = await db.sum_call_minutes_for_day(d)
            for r in rows:
                await db.upsert_call_minutes(d, r["user_id"], int(r["mins"] or 0))
            await send_daily_reports_to_guard(d)
        except Exception:
            await asyncio.sleep(60)

async def send_daily_reports_to_guard(d: date):
    # CHAT
    async with db.pool.acquire() as con:
        chat_rows = await con.fetch("""
            SELECT d.user_id, d.chat_msgs, d.mentions, u.first_name, u.last_name
            FROM daily_stats d JOIN users u ON u.user_id=d.user_id
            WHERE d.day=$1 AND (d.chat_msgs>0 OR d.mentions>0)
            ORDER BY d.chat_msgs DESC
        """, d)
    if chat_rows:
        lines = [f"گزارش چت مدیران - {fmt_jalali(d)}"]
        for r in chat_rows:
            name = ((r['first_name'] or '') + ' ' + (r['last_name'] or '')).strip()
            lines.append(f"{mention_html(r['user_id'], name or str(r['user_id']))} — پیام‌ها: {r['chat_msgs']} | منشن: {r['mentions']}")
        await bot.send_message(GUARD_CHAT_ID, "\n".join(lines), disable_web_page_preview=True)

    # CALL
    async with db.pool.acquire() as con:
        call_rows = await con.fetch("""
            SELECT d.user_id, d.call_minutes, u.first_name, u.last_name
            FROM daily_stats d JOIN users u ON u.user_id=d.user_id
            WHERE d.day=$1 AND d.call_minutes>0
            ORDER BY d.call_minutes DESC
        """, d)
    if call_rows:
        lines = [f"گزارش کال مدیران - {fmt_jalali(d)}"]
        for r in call_rows:
            name = ((r['first_name'] or '') + ' ' + (r['last_name'] or '')).strip()
            lines.append(f"{mention_html(r['user_id'], name or str(r['user_id']))} — {int(r['call_minutes'])} دقیقه")
        await bot.send_message(GUARD_CHAT_ID, "\n".join(lines), disable_web_page_preview=True)

# ---------- GAMES (10+) ----------
def game_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 حدس عدد 1..100", callback_data="g:number:100"),
         InlineKeyboardButton(text="🎯 حدس عدد 1..1000", callback_data="g:number:1000")],
        [InlineKeyboardButton(text="⌨️ تایپ سرعتی", callback_data="g:typing"),
         InlineKeyboardButton(text="✊✋✌️ سنگ‌کاغذقیچی", callback_data="g:rps")],
        [InlineKeyboardButton(text="➗ ریاضی سریع", callback_data="g:math"),
         InlineKeyboardButton(text="🔤 حدس کلمه", callback_data="g:word")],
        [InlineKeyboardButton(text="🧩 تکمیل کلمه", callback_data="g:fill")],
        [InlineKeyboardButton(text="🔀 درهم‌ریخته", callback_data="g:anagram"),
         InlineKeyboardButton(text="✅❌ درست/نادرست", callback_data="g:tf")],
        [InlineKeyboardButton(text="🧠 حافظه عددی", callback_data="g:memory")]
    ])

WORDS = ["PYTHON","TELEGRAM","SOULS","GUARD","ADMIN","VOICE","CHAT","HUMOR","SECURITY","FRIEND"]
PERSIAN_WORDS = ["ربات","سولز","گارد","مدیر","حضور","آمار","تماس","پروفایل","کال","گفتگو"]

@r_grp.message(F.text.regexp("^بازی$"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def game_cmd(msg: Message):
    await msg.reply("یک بازی انتخاب کن:", reply_markup=game_menu_kb())

# start handlers
@r_grp.callback_query(F.data.startswith("g:number:"))
async def g_number_start(cb: CallbackQuery):
    limit = int(cb.data.split(":")[2])
    GAME_STATE[cb.from_user.id] = {"type":"number","n":random.randint(1,limit),"limit":limit}
    await cb.message.reply(f"یک عدد بین 1 تا {limit} حدس بزن. (با ارسال عدد)"); await cb.answer()

@r_grp.callback_query(F.data=="g:typing")
async def g_typing(cb: CallbackQuery):
    word = "".join(random.choice(string.ascii_letters) for _ in range(6))
    GAME_STATE[cb.from_user.id] = {"type":"typing","word":word,"start":datetime.utcnow()}
    await cb.message.reply(f"این را سریع تایپ کن: <code>{word}</code>"); await cb.answer()

@r_grp.callback_query(F.data=="g:rps")
async def g_rps(cb: CallbackQuery):
    GAME_STATE[cb.from_user.id] = {"type":"rps","await":True}
    await cb.message.reply("یکی را بفرست: سنگ / کاغذ / قیچی"); await cb.answer()

@r_grp.callback_query(F.data=="g:math")
async def g_math(cb: CallbackQuery):
    a,b = random.randint(2,20), random.randint(2,20)
    op = random.choice(["+","-","*"])
    ans = eval(f"{a}{op}{b}")
    GAME_STATE[cb.from_user.id] = {"type":"math","ans":ans}
    await cb.message.reply(f"حل کن: {a} {op} {b} = ?"); await cb.answer()

@r_grp.callback_query(F.data=="g:word")
async def g_word(cb: CallbackQuery):
    w = random.choice(WORDS)
    hint = w[0] + ("_"*(len(w)-2)) + w[-1]
    GAME_STATE[cb.from_user.id] = {"type":"word","word":w}
    await cb.message.reply(f"حدس کلمه (لاتین): {hint}"); await cb.answer()

@r_grp.callback_query(F.data=="g:fill")
async def g_fill(cb: CallbackQuery):
    w = random.choice(PERSIAN_WORDS)
    idx = random.randrange(len(w))
    masked = w[:idx] + "‌_" + w[idx+1:]
    GAME_STATE[cb.from_user.id] = {"type":"fill","word":w}
    await cb.message.reply(f"حرف جاافتاده را کامل کن: {masked}"); await cb.answer()

@r_grp.callback_query(F.data=="g:anagram")
async def g_anagram(cb: CallbackQuery):
    w = random.choice(WORDS)
    letters = list(w); random.shuffle(letters)
    GAME_STATE[cb.from_user.id] = {"type":"anagram","word":w}
    await cb.message.reply(f"کلمهٔ به‌هم‌ریخته را درست کن: {' '.join(letters)}"); await cb.answer()

TF_QUESTIONS = [
    ("تهران پایتخت ایرانه.", True),
    ("2+2=5", False),
    ("Python زبان کاملاً کامپایلری است.", False),
    ("خورشید یک ستاره است.", True),
    ("تلگرام متعلق به گوگل است.", False)
]

@r_grp.callback_query(F.data=="g:tf")
async def g_tf(cb: CallbackQuery):
    q, a = random.choice(TF_QUESTIONS)
    GAME_STATE[cb.from_user.id] = {"type":"tf","ans":a}
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ درسته", callback_data="tf:1"),
                                                InlineKeyboardButton(text="❌ غلطه", callback_data="tf:0")]])
    await cb.message.reply(q, reply_markup=kb); await cb.answer()

@r_grp.callback_query(F.data.startswith("tf:"))
async def g_tf_answer(cb: CallbackQuery):
    if GAME_STATE.get(cb.from_user.id,{}).get("type")!="tf": await cb.answer(); return
    ans = GAME_STATE[cb.from_user.id]["ans"]; pick = cb.data.endswith("1")
    del GAME_STATE[cb.from_user.id]
    await cb.message.reply("👏 درست گفتی!" if pick==ans else "نه، اشتباه بود!"); await cb.answer()

@r_grp.callback_query(F.data=="g:memory")
async def g_memory(cb: CallbackQuery):
    seq = "".join(str(random.randint(0,9)) for _ in range(6))
    GAME_STATE[cb.from_user.id] = {"type":"memory","seq":seq}
    await cb.message.reply(f"این عدد را حفظ کن و بعد تکرار کن: <code>{seq}</code>"); await cb.answer()

# message router for game inputs
@r_grp.message(F.text, F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def game_handler(msg: Message):
    state = GAME_STATE.get(msg.from_user.id)
    if not state: return
    t = state.get("type")
    if t=="number":
        try: g = int(msg.text.strip())
        except ValueError: return
        if g==state["n"]:
            del GAME_STATE[msg.from_user.id]; await msg.reply(f"درست حدس زدی! 👏 عدد {state['n']} بود.")
        elif g<state["n"]: await msg.reply("بزرگتر 🙂")
        else: await msg.reply("کوچکتر 🙂")
    elif t=="typing":
        if msg.text.strip()==state["word"]:
            delta = (datetime.utcnow()-state["start"]).total_seconds()
            del GAME_STATE[msg.from_user.id]; await msg.reply(f"عالی! در {delta:.2f} ثانیه تایپ کردی.")
    elif t=="rps":
        pick = msg.text.strip()
        choices = ["سنگ","کاغذ","قیچی"]
        if pick not in choices: return
        botp = random.choice(choices)
        res = "مساوی!"
        if (pick=="سنگ" and botp=="قیچی") or (pick=="کاغذ" and botp=="سنگ") or (pick=="قیچی" and botp=="کاغذ"):
            res = "بردی! 🎉"
        elif pick!=botp:
            res = "باختی! 😅"
        del GAME_STATE[msg.from_user.id]
        await msg.reply(f"تو: {pick} | ربات: {botp} → {res}")
    elif t=="math":
        try: g = int(msg.text.strip())
        except ValueError: return
        ans = state["ans"]; del GAME_STATE[msg.from_user.id]
        await msg.reply("👏 درست بود." if g==ans else f"غلط بود! جواب {ans}")
    elif t=="word":
        if msg.text.strip().upper()==state["word"]:
            del GAME_STATE[msg.from_user.id]; await msg.reply("آفرین! درست گفتی 👏")
    elif t=="fill":
        if msg.text.strip()==state["word"]:
            del GAME_STATE[msg.from_user.id]; await msg.reply("عالی! کامل شد 👏")
    elif t=="anagram":
        if msg.text.strip().upper()==state["word"]:
            del GAME_STATE[msg.from_user.id]; await msg.reply("درست مرتب کردی! 👏")
    elif t=="memory":
        if msg.text.strip()==state["seq"]:
            del GAME_STATE[msg.from_user.id]; await msg.reply("حافظه‌ات عالیه! 🧠")
        else:
            del GAME_STATE[msg.from_user.id]; await msg.reply(f"اشتباه بود! عدد {state['seq']} بود.")

# ---------- OWNER/ADMINS "ربات" NICE REPLIES ----------
NICE_REPLIES = [f"جمله قشنگ {i}" for i in range(1,101)] + ["در خدمتم رئیس 🤝","باعث افتخاره 🌟","چشم👌","همین الان!","با قدرت ✌️"]

@r_grp.message(F.text.regexp("^ربات$"), F.chat.id.in_([MAIN_CHAT_ID, GUARD_CHAT_ID]))
async def pretty_bot(msg: Message):
    role = await db.get_role(msg.from_user.id)
    if msg.from_user.id==OWNER_ID or role in ("lead_all","lead_chat","lead_call","admin_chat","admin_call"):
        await msg.reply(random.choice(NICE_REPLIES))

# ---------- MEMBER UPDATES ----------
@r_grp.chat_member()
async def on_member_update(event: ChatMemberUpdated):
    if event.from_user: await db.upsert_user(event.from_user)

# ---------- RUN ----------
async def main():
    await db.connect()
    asyncio.create_task(daily_aggregation_loop())
    asyncio.create_task(random_tag_loop())
    print("Bot is running...")
    await dp.start_polling(bot, allowed_updates=["message","callback_query","chat_member"])

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): print("Bot stopped")
