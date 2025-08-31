# main.py — Souls Security Bot (Railway + PostgreSQL)
# PTB v20.x (async)
# ENV: BOT_TOKEN, DATABASE_URL, MAIN_CHAT_ID, GUARD_CHAT_ID, OWNER_ID, TZ=Asia/Tehran
# Optional: FUN_LINES_FILE=/app/fun_lines.txt   (یک جمله در هر خط)

import os
import re
import random
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo

import asyncpg
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatPermissions,
)
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    Application,
    ContextTypes,
    MessageHandler,
    CommandHandler,
    CallbackQueryHandler,
    filters,
    AIORateLimiter,
)

# -------------------- Config --------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").replace("postgres://", "postgresql://")
MAIN_CHAT_ID = int(os.environ.get("MAIN_CHAT_ID", "0"))
GUARD_CHAT_ID = int(os.environ.get("GUARD_CHAT_ID", "0"))
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
TZ = os.environ.get("TZ", "Asia/Tehran")
TZINFO = ZoneInfo(TZ)

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")

# -------------------- Helpers --------------------
def now() -> datetime:
    return datetime.now(TZINFO)

def today() -> date:
    return now().date()

def human_td(seconds: int) -> str:
    s = int(seconds or 0)
    h = s // 3600
    m = (s % 3600) // 60
    s2 = s % 60
    parts = []
    if h: parts.append(f"{h}ساعت")
    if m: parts.append(f"{m}دقیقه")
    if s2 and not parts: parts.append(f"{s2}ثانیه")
    return " ".join(parts) or "0"

def mention_html(user_or_id):
    if hasattr(user_or_id, "id"):
        uid = user_or_id.id
        name = (user_or_id.first_name or "") + (" " + user_or_id.last_name if getattr(user_or_id, "last_name", None) else "")
        name = name.strip() or (getattr(user_or_id, "username", None) and "@" + user_or_id.username) or str(uid)
    else:
        uid = int(user_or_id)
        name = str(uid)
    return f'<a href="tg://user?id={uid}">{name}</a>'

def is_owner(uid: int) -> bool:
    return uid == OWNER_ID

async def try_clear_kb(message):
    try:
        await message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

# -------------------- SQL Schema --------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS config (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    random_tag BOOLEAN NOT NULL DEFAULT FALSE,
    gender_prompt BOOLEAN NOT NULL DEFAULT TRUE
);
INSERT INTO config(id) VALUES (TRUE) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    gender TEXT CHECK (gender IN ('male','female') OR gender IS NULL),
    role TEXT,       -- chat_admin, call_admin, channel_admin, senior_chat, senior_call, senior_all
    rank INT DEFAULT 0,
    joined_guard_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS banned_users ( user_id BIGINT PRIMARY KEY );

CREATE TABLE IF NOT EXISTS daily_stats (
    d DATE NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    chat_messages INT NOT NULL DEFAULT 0,
    replies_sent INT NOT NULL DEFAULT 0,
    replies_received INT NOT NULL DEFAULT 0,
    chat_seconds INT NOT NULL DEFAULT 0,
    call_seconds INT NOT NULL DEFAULT 0,
    call_sessions INT NOT NULL DEFAULT 0,
    first_checkin TIMESTAMPTZ,
    last_checkout TIMESTAMPTZ,
    PRIMARY KEY (d, user_id)
);
CREATE INDEX IF NOT EXISTS idx_daily_stats_d ON daily_stats(d);

CREATE TABLE IF NOT EXISTS sessions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('chat','call')),
    start_ts TIMESTAMPTZ NOT NULL,
    last_activity_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ,
    open_msg_id BIGINT,
    open_msg_chat BIGINT,
    msg_count INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS members_stats (
    d DATE NOT NULL,
    user_id BIGINT NOT NULL,
    chat_count INT NOT NULL DEFAULT 0,
    last_active TIMESTAMPTZ,
    PRIMARY KEY (d, user_id)
);

CREATE TABLE IF NOT EXISTS ratings (
    d DATE NOT NULL,
    rater_id BIGINT NOT NULL,
    rating BOOLEAN NOT NULL,
    PRIMARY KEY (d, rater_id)
);

CREATE TABLE IF NOT EXISTS watchlist ( user_id BIGINT PRIMARY KEY );

CREATE TABLE IF NOT EXISTS contact_threads (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('guard','owner')),
    status TEXT NOT NULL DEFAULT 'open',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_forwarded_msg BIGINT,
    last_forwarded_chat BIGINT
);

CREATE TABLE IF NOT EXISTS admin_requests (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'open'
);

-- ensure columns exist on old deployments
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS msg_count INT NOT NULL DEFAULT 0;
ALTER TABLE config ADD COLUMN IF NOT EXISTS gender_prompt BOOLEAN NOT NULL DEFAULT TRUE;
"""

# -------------------- Fun lines (loaded from file if provided) --------------------
DEFAULT_FUN_LINES = [
    # فهرست امن و تمیز. اگر بخواهی، از فایل بیرونی لود می‌شود (FUN_LINES_FILE).
    "یادت نره آب بخوری! 💧",
    "امروزت پر از انرژی مثبت باشه ✨",
    "یه لبخند کوچیک می‌تونه روزتو عوض کنه 🙂",
    "همین الان یه نفس عمیق بکش 😌",
    "یه استراحت کوتاه لازمه ☕️",
    "بچه‌های سولز پشتتن 😉",
    "امروز بهترین نسخه خودت باش 🌟",
    "بهت افتخار می‌کنیم 👏",
    "یه آهنگ خوب گوش بده 🎶",
    "یه لیوان چای داغ می‌چسبه 🍵",
    "ذهن آروم = زندگی قشنگ 🧘",
    "شاد بودن انتخابه، انتخاب کن 😎",
    "لبخند بزن، حتی وقتی سخت میشه 🌻",
    "موفقیت با تلاش میاد 🛠️",
    "یه موزیک شاد پلی کن 🎧",
]

def load_fun_lines() -> list[str]:
    path = os.environ.get("FUN_LINES_FILE")
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f if ln.strip()]
            return lines[:200]
        except Exception:
            pass
    return DEFAULT_FUN_LINES

FUN_LINES = load_fun_lines()

# -------------------- DB --------------------
class DB:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)
        async with self.pool.acquire() as con:
            await con.execute(SCHEMA_SQL)

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def fetch(self, q, *a):
        async with self.pool.acquire() as c:
            return await c.fetch(q, *a)

    async def fetchrow(self, q, *a):
        async with self.pool.acquire() as c:
            return await c.fetchrow(q, *a)

    async def execute(self, q, *a):
        async with self.pool.acquire() as c:
            return await c.execute(q, *a)

db = DB(DATABASE_URL)

# -------------------- Keyboards --------------------
def kb_checkin():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ ورود چت", callback_data="checkin_chat"),
         InlineKeyboardButton("🎧 ورود کال", callback_data="checkin_call")]
    ])

def kb_owner_rate():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("👍 راضی", callback_data="rate_yes"),
        InlineKeyboardButton("👎 ناراضی", callback_data="rate_no")
    ]])

def kb_reply_block(thread_id: int):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("پاسخ", callback_data=f"reply_{thread_id}"),
        InlineKeyboardButton("مسدود", callback_data=f"block_{thread_id}")
    ]])

def kb_back_retry():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("↩️ بازگشت", callback_data="back_home"),
        InlineKeyboardButton("🔄 ارسال مجدد", callback_data="retry_send")
    ]])

def kb_gender(uid: int):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("👦 پسر", callback_data=f"gender_male_{uid}"),
        InlineKeyboardButton("👧 دختر", callback_data=f"gender_female_{uid}")
    ]])

HOME_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("🛡️ ارتباط با گارد مدیران", callback_data="contact_guard")],
    [InlineKeyboardButton("👤 ارتباط با مالک", callback_data="contact_owner")],
    [InlineKeyboardButton("📊 آمار من", callback_data="my_stats")]
])

WELCOME_TEXT = (
    "سلام! این ربات ویژه مالک تیم <b>Souls</b> است.\n"
    "برای ارتباط با گارد مدیران یا مالک از دکمه‌ها استفاده کنید."
)

# -------------------- Utility --------------------
async def ensure_user(u):
    await db.execute(
        """INSERT INTO users(user_id,first_name,last_name,username)
           VALUES($1,$2,$3,$4)
           ON CONFLICT (user_id) DO UPDATE SET
             first_name=EXCLUDED.first_name,
             last_name=EXCLUDED.last_name,
             username=EXCLUDED.username""",
        u.id, u.first_name, u.last_name, u.username
    )

async def is_guard_member(uid: int) -> bool:
    if is_owner(uid):
        return True
    row = await db.fetchrow("SELECT role FROM users WHERE user_id=$1", uid)
    return bool(row and row["role"])

async def bump_member_stats(uid: int):
    await db.execute(
        """INSERT INTO members_stats(d,user_id,chat_count,last_active)
           VALUES($1,$2,1,$3)
           ON CONFLICT (d,user_id) DO UPDATE SET
             chat_count=members_stats.chat_count+1, last_active=$3""",
        today(), uid, now()
    )

async def bump_admin_on_message(message):
    uid = message.from_user.id
    d = today()
    await db.execute(
        """INSERT INTO daily_stats(d,user_id,chat_messages)
           VALUES($1,$2,1)
           ON CONFLICT (d,user_id) DO UPDATE SET chat_messages=daily_stats.chat_messages+1""",
        d, uid
    )
    if message.reply_to_message and message.reply_to_message.from_user:
        await db.execute(
            """INSERT INTO daily_stats(d,user_id,replies_sent)
               VALUES($1,$2,1)
               ON CONFLICT (d,user_id) DO UPDATE SET replies_sent=daily_stats.replies_sent+1""",
            d, uid
        )
        orig = message.reply_to_message.from_user.id
        await db.execute(
            """INSERT INTO daily_stats(d,user_id,replies_received)
               VALUES($1,$2,1)
               ON CONFLICT (d,user_id) DO UPDATE SET replies_received=daily_stats.replies_received+1""",
            d, orig
        )
    # شمارش پیام نوبت چت (اگر باز است)
    sess = await get_open_session(uid, "chat")
    if sess:
        await db.execute("UPDATE sessions SET msg_count = msg_count + 1, last_activity_ts=$1 WHERE id=$2", now(), sess["id"])

async def get_open_session(uid: int, kind: str | None = None):
    if kind:
        return await db.fetchrow(
            "SELECT * FROM sessions WHERE user_id=$1 AND kind=$2 AND end_ts IS NULL ORDER BY id DESC LIMIT 1",
            uid, kind
        )
    return await db.fetchrow(
        "SELECT * FROM sessions WHERE user_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1",
        uid
    )

async def start_session(context: ContextTypes.DEFAULT_TYPE, uid: int, kind: str, msg_chat=None, msg_id=None):
    ex = await get_open_session(uid, kind)
    if ex:
        await db.execute("UPDATE sessions SET last_activity_ts=$1 WHERE id=$2", now(), ex["id"])
        return ex["id"]
    rec = await db.fetchrow(
        """INSERT INTO sessions(user_id,kind,start_ts,last_activity_ts,open_msg_chat,open_msg_id)
           VALUES($1,$2,$3,$3,$4,$5) RETURNING id""",
        uid, kind, now(), msg_chat, msg_id
    )
    await db.execute(
        """INSERT INTO daily_stats(d,user_id,first_checkin)
           VALUES($1,$2,$3)
           ON CONFLICT (d,user_id) DO UPDATE SET first_checkin=COALESCE(daily_stats.first_checkin,$3)""",
        today(), uid, now()
    )
    if kind == "chat":
        await schedule_inactivity(context, rec["id"])  # 5-minute idle watcher
    return rec["id"]

async def end_session(context: ContextTypes.DEFAULT_TYPE, sess_id: int, reason="manual"):
    sess = await db.fetchrow("SELECT * FROM sessions WHERE id=$1", sess_id)
    if not sess or sess["end_ts"]:
        return
    end_ts = now()
    await db.execute("UPDATE sessions SET end_ts=$1 WHERE id=$2", end_ts, sess_id)
    dur = int((end_ts - sess["start_ts"]).total_seconds())
    col = "chat_seconds" if sess["kind"] == "chat" else "call_seconds"
    inc_call = ", call_sessions = daily_stats.call_sessions + 1" if sess["kind"] == "call" else ""
    await db.execute(
        f"""INSERT INTO daily_stats(d,user_id,{col},last_checkout)
            VALUES($1,$2,$3,$4)
            ON CONFLICT (d,user_id) DO UPDATE SET
              {col}=daily_stats.{col}+$3, last_checkout=$4 {inc_call}""",
        today(), sess["user_id"], dur, end_ts
    )
    # گزارش خروج
    txt = (f"{'⛔️' if reason!='manual' else '❌'} خروج {('چت' if sess['kind']=='chat' else 'کال')}\n"
           f"مدت: {human_td(dur)}\n"
           f"تعداد پیام در این نوبت: {sess['msg_count']}")
    for ch in [GUARD_CHAT_ID, OWNER_ID]:
        try:
            await context.bot.send_message(ch, txt)
        except Exception:
            pass
    try:
        await context.bot.send_message(sess["user_id"], txt)
    except Exception:
        pass

async def end_all_sessions(context: ContextTypes.DEFAULT_TYPE, uid: int, reason="manual"):
    rows = await db.fetch("SELECT id FROM sessions WHERE user_id=$1 AND end_ts IS NULL", uid)
    for r in rows:
        await end_session(context, r["id"], reason=reason)

async def schedule_inactivity(context: ContextTypes.DEFAULT_TYPE, sess_id: int):
    # job هر 60 ثانیه چک می‌کند اگر 5 دقیقه بی‌فعالی → خروج خودکار (فقط چت)
    name = f"inact_{sess_id}"
    for j in context.job_queue.get_jobs_by_name(name):
        j.schedule_removal()
    context.job_queue.run_repeating(inactivity_job, interval=60, first=60, name=name, data={"sess_id": sess_id})

async def inactivity_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    sid = data.get("sess_id")
    sess = await db.fetchrow("SELECT * FROM sessions WHERE id=$1", sid)
    if not sess or sess["end_ts"]:
        context.job.schedule_removal(); return
    if sess["kind"] != "chat":
        context.job.schedule_removal(); return
    if now() - sess["last_activity_ts"].astimezone(TZINFO) >= timedelta(minutes=5):
        await end_session(context, sid, reason="بدون فعالیت ۵ دقیقه")
        context.job.schedule_removal()

# -------------------- Role & mention helpers --------------------
async def get_role(uid: int) -> str | None:
    row = await db.fetchrow("SELECT role FROM users WHERE user_id=$1", uid)
    return row["role"] if row else None

async def is_senior_or_owner(uid: int) -> bool:
    if is_owner(uid):
        return True
    role = await get_role(uid)
    return role in ("senior_all", "senior_chat", "senior_call")

async def resolve_display_name(context: ContextTypes.DEFAULT_TYPE, uid: int) -> str:
    row = await db.fetchrow("SELECT first_name, last_name, username FROM users WHERE user_id=$1", uid)
    if row:
        fn = row["first_name"] or ""
        ln = row["last_name"] or ""
        name = (fn + (" " + ln if ln else "")).strip()
        if name:
            return name
        if row["username"]:
            return "@" + row["username"]
    try:
        cm = await context.bot.get_chat_member(MAIN_CHAT_ID, uid)
        fn = cm.user.first_name or ""
        ln = cm.user.last_name or ""
        name = (fn + (" " + ln if ln else "")).strip() or (cm.user.username and "@" + cm.user.username)
        return name or str(uid)
    except Exception:
        return str(uid)

async def mention_name(context: ContextTypes.DEFAULT_TYPE, uid: int) -> str:
    name = await resolve_display_name(context, uid)
    return f'<a href="tg://user?id={uid}">{name}</a>'

# -------------------- Start & Home --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    if update.message:
        await update.message.reply_html(WELCOME_TEXT, reply_markup=HOME_KB)

# -------------------- Contact flow --------------------
def kb_contact_home():
    return HOME_KB

async def on_contact_btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data in ("contact_guard","contact_owner"):
        channel = "guard" if q.data.endswith("guard") else "owner"
        context.user_data["contact_channel"] = channel
        await try_clear_kb(q.message)
        await q.message.reply_text(
            f"پیام خود را برای {'گارد مدیران' if channel=='guard' else 'مالک'} ارسال کنید.\nمتن/عکس/ویس مجاز است.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="back_home"),
                                                InlineKeyboardButton("🔄 ارسال مجدد", callback_data="retry_send")]])
        )
    elif q.data == "back_home":
        await try_clear_kb(q.message)
        await q.message.reply_text(WELCOME_TEXT, reply_markup=kb_contact_home())
    elif q.data == "retry_send":
        await q.answer("پیام جدید بفرستید.", show_alert=True)

async def pipe_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    channel = context.user_data.get("contact_channel")
    if not channel: return
    u = update.effective_user
    await ensure_user(u)
    dest = GUARD_CHAT_ID if channel == "guard" else OWNER_ID
    caption = f"کاربر: {mention_html(u)}\nID: <code>{u.id}</code>"
    sent = None
    try:
        if update.message.photo:
            sent = await context.bot.send_photo(dest, update.message.photo[-1].file_id, caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb_reply_block(0))
        elif update.message.voice:
            sent = await context.bot.send_voice(dest, update.message.voice.file_id, caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb_reply_block(0))
        elif update.message.text:
            sent = await context.bot.send_message(dest, f"{caption}\n\n{update.message.text_html}", parse_mode=ParseMode.HTML, reply_markup=kb_reply_block(0))
        else:
            sent = await context.bot.send_message(dest, caption+"\n(نوع رسانه پشتیبانی نشد)", parse_mode=ParseMode.HTML, reply_markup=kb_reply_block(0))
    except Exception:
        if update.message: await update.message.reply_text("ارسال ناموفق بود.")
        return
    if sent:
        rec = await db.fetchrow(
            "INSERT INTO contact_threads(user_id,channel,last_forwarded_msg,last_forwarded_chat) VALUES($1,$2,$3,$4) RETURNING id",
            u.id, channel, sent.message_id, dest
        )
        try: await sent.edit_reply_markup(kb_reply_block(rec["id"]))
        except Exception: pass
        await update.message.reply_text("پیام شما ارسال شد ✅",
                                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="back_home"),
                                                                            InlineKeyboardButton("🔄 ارسال مجدد", callback_data="retry_send")]]))

async def on_guard_reply_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data.startswith("block_"):
        tid = int(data.split("_", 1)[1])
        rec = await db.fetchrow("SELECT * FROM contact_threads WHERE id=$1", tid)
        if not rec:
            return
        await db.execute("INSERT INTO banned_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", rec["user_id"])
        await try_clear_kb(q.message)
        await q.message.reply_text("کاربر مسدود شد.")
        return

    if data.startswith("reply_"):
        tid = int(data.split("_", 1)[1])
        context.user_data["one_shot_reply_tid"] = tid
        await try_clear_kb(q.message)
        await q.message.reply_text(
            "پیام پاسخ خود را ارسال کنید.\n"
            "⚠️ فقط اولین پیام بعد از این کلیک فوروارد می‌شود. "
            "برای پاسخ جدید دوباره دکمه «پاسخ» را بزنید."
        )
        return

async def capture_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid = context.user_data.pop("one_shot_reply_tid", None)
    if not tid:
        return
    rec = await db.fetchrow("SELECT * FROM contact_threads WHERE id=$1", tid)
    if not rec:
        await update.message.reply_text("ترد نامعتبر است. دوباره دکمه «پاسخ» را بزنید.")
        return
    uid = rec["user_id"]
    m = update.message
    try:
        if m.text:
            await context.bot.send_message(uid, f"پاسخ مدیریت:\n\n{m.text}")
        elif m.photo:
            await context.bot.send_photo(uid, m.photo[-1].file_id, caption="پاسخ مدیریت:")
        elif m.voice:
            await context.bot.send_voice(uid, m.voice.file_id, caption="پاسخ مدیریت:")
        elif m.document:
            await context.bot.send_document(uid, m.document.file_id, caption="پاسخ مدیریت:")
        elif m.video:
            await context.bot.send_video(uid, m.video.file_id, caption="پاسخ مدیریت:")
        elif m.animation:
            await context.bot.send_animation(uid, m.animation.file_id, caption="پاسخ مدیریت:")
        else:
            await context.bot.send_message(uid, "پاسخ مدیریت ارسال شد.")
        await m.reply_text("پاسخ ارسال شد ✅\nبرای پاسخ جدید، دوباره دکمه «پاسخ» را بزنید.")
        try:
            await context.bot.edit_message_reply_markup(
                rec["last_forwarded_chat"], rec["last_forwarded_msg"],
                reply_markup=kb_reply_block(rec["id"])
            )
        except Exception:
            pass
    except Exception:
        await m.reply_text("ارسال پاسخ ناموفق بود. دوباره تلاش کنید.")

# -------------------- Rating buttons --------------------
async def on_owner_rate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("فقط مالک!", show_alert=True); return
    await q.answer()
    val = True if q.data.endswith("yes") else False
    await db.execute(
        "INSERT INTO ratings(d,rater_id,rating) VALUES($1,$2,$3) ON CONFLICT (d,rater_id) DO UPDATE SET rating=$3",
        today(), OWNER_ID, val
    )
    await q.message.reply_text("ثبت شد.")

# -------------------- Checkin/Checkout callbacks --------------------
async def on_checkin_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = q.from_user
    await ensure_user(u)
    await try_clear_kb(q.message)

    if q.data == "checkin_chat":
        txt = f"✅ ورود چت: {await mention_name(context, u.id)}"
        for dest in (GUARD_CHAT_ID, OWNER_ID):
            try: await context.bot.send_message(dest, txt, parse_mode=ParseMode.HTML)
            except Exception: pass
        await start_session(context, u.id, "chat")
        try: await q.message.edit_text("✅ فعالیت چت ثبت شد.", parse_mode=ParseMode.HTML)
        except Exception: pass
        try: await context.bot.send_message(u.id, "ورود چت ثبت شد ✅")
        except Exception: pass

    elif q.data == "checkin_call":
        txt = f"🎧 ورود کال: {await mention_name(context, u.id)}"
        for dest in (GUARD_CHAT_ID, OWNER_ID):
            try: await context.bot.send_message(dest, txt, parse_mode=ParseMode.HTML)
            except Exception: pass
        await start_session(context, u.id, "call")
        try: await q.message.edit_text("🎧 فعالیت کال ثبت شد.", parse_mode=ParseMode.HTML)
        except Exception: pass
        try: await context.bot.send_message(u.id, "ورود کال ثبت شد ✅")
        except Exception: pass

    elif q.data.startswith("checkout_"):
        await q.message.reply_text("برای خروج، دستور «ثبت خروج» را ارسال کنید.")
        return

    elif q.data in ("switch_to_chat","switch_to_call"):
        target = "chat" if q.data.endswith("chat") else "call"
        other = "call" if target=="chat" else "chat"
        old = await get_open_session(u.id, other)
        if old: await end_session(context, old["id"], reason="تغییر فعالیت")
        txt = f"🔁 تغییر فعالیت به {('چت' if target=='chat' else 'کال')}: {await mention_name(context, u.id)}"
        for dest in (GUARD_CHAT_ID, OWNER_ID):
            try: await context.bot.send_message(dest, txt, parse_mode=ParseMode.HTML)
            except Exception: pass
        await start_session(context, u.id, target)
        try: await q.message.edit_text(f"🔁 تغییر فعالیت به {'چت' if target=='chat' else 'کال'} ثبت شد.", parse_mode=ParseMode.HTML)
        except Exception: pass
        try: await context.bot.send_message(u.id, "تغییر فعالیت ثبت شد ✅")
        except Exception: pass

# -------------------- Gender choice callbacks --------------------
async def on_gender_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if not (data.startswith("gender_male_") or data.startswith("gender_female_")):
        return
    try:
        _, typ, uid_suf = data.split("_", 2)
    except ValueError:
        # pattern is gender_male_<uid> OR gender_female_<uid>
        typ = "male" if data.startswith("gender_male_") else "female"
        uid_suf = data.split("_")[-1]
    target_uid = int(uid_suf)
    clicker = q.from_user.id

    # فقط خودِ کاربر هدف یا اعضای گارد اجازه دارند
    if clicker != target_uid and not await is_guard_member(clicker):
        await q.answer("اجازه ندارید.", show_alert=True); return

    await db.execute("INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING", target_uid)
    await db.execute("UPDATE users SET gender=$2 WHERE user_id=$1", target_uid, typ if typ in ("male","female") else ("male" if "male" in data else "female"))

    # ادیت/حذف دکمه‌ها
    try:
        await q.message.edit_text(f"جنسیت {await mention_name(context, target_uid)} ثبت شد.", parse_mode=ParseMode.HTML)
    except Exception:
        await try_clear_kb(q.message)

# -------------------- My stats --------------------
async def on_my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    r = await db.fetchrow("SELECT * FROM daily_stats WHERE d=$1 AND user_id=$2", today(), uid)
    if not r:
        await q.message.reply_text("امروز آماری ندارید."); return
    txt = (f"آمار امروز:\n"
           f"- پیام‌های چت: {r['chat_messages']}\n"
           f"- ریپلای زده/دریافت: {r['replies_sent']}/{r['replies_received']}\n"
           f"- زمان حضور چت: {human_td(r['chat_seconds'])}\n"
           f"- زمان فعالیت کال: {human_td(r['call_seconds'])}\n"
           f"- دفعات کال: {r['call_sessions']}")
    await q.message.reply_text(txt)

# -------------------- Text triggers (no slash) --------------------
RE_RANDOM_TAG = {"تگ رندوم روشن": True, "تگ رندوم خاموش": False}
RE_GENDER_PROMPT = {"جنسیت روشن": True, "جنسیت خاموش": False}

def extract_target_from_text_or_reply(update: Update):
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        return update.message.reply_to_message.from_user.id
    m = re.search(r"(\d{4,})", update.message.text or "")
    return int(m.group(1)) if m else None

OWNER_HELP = (
    "راهنما (بدون /):\n"
    "• ثبت — پاپ‌آپ ثبت فعالیت (چت/کال).\n"
    "• ورود چت / ورود کال — متنی (خود/با ریپلای/آیدی برای دیگران).\n"
    "• ثبت خروج — تمام سشن‌های باز را می‌بندد (خود/با ریپلای/آیدی).\n"
    "• تغییر فعالیت — پاپ‌آپ تغییر بین چت/کال.\n"
    "• ترفیع/عزل چت، کال، ارشدچت، ارشدکال، ارشدکل، کانال — با ریپلای/آیدی (مالک).\n"
    "• محدود رسانه / آزاد رسانه — فقط متن‌دادن یا رفع محدودیت (با ریپلای/آیدی).\n"
    "• سکوت / حذف سکوت — در گروه اصلی و با ریپلای.\n"
    "• ثبت پسر / ثبت دختر — برای خود یا با ریپلای تعیین جنسیت کاربر.\n"
    "• تگ پسرها / تگ دخترها / تگ لیست پسر / تگ لیست دختر / تگ لیست همه — با ریپلای روی پیام.\n"
    "• گارد — وضعیت امروز شما.\n"
    "• آمار — تعداد کاربران فعال امروز.\n"
    "• آمار چت الان / آمار کال الان — تا این لحظه برای تیم مدیریت (با دکمه رضایت).\n"
    "• آمار کلی کاربر <آیدی> — گزارش ۳۰ روز گذشته.\n"
    "• ممنوع <آیدی> / آزاد <آیدی> — لیست ممنوع.\n"
    "• زیرنظر+<آیدی> — گزارش شبانهٔ اختصاصی.\n"
    "• تگ رندوم روشن / تگ رندوم خاموش — منشن تصادفی هر ۱۵ دقیقه.\n"
    "• جنسیت روشن / جنسیت خاموش — پاپ‌آپ تعیین جنسیت برای اولین پیام کاربران.\n"
    "• لیست گارد — سلسله‌مراتب مدیریت (مالک در صدر).\n"
    "• درخواست ادمینی — گزارش امروز و ۷ روز اخیر کاربر برای مالک و گارد.\n"
)

async def text_triggers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    txt = update.message.text.strip()
    user = update.effective_user
    chat_id = update.effective_chat.id

    # مالک/ارشد تنظیمات
    if is_owner(user.id):
        if txt == "راهنما":
            await update.message.reply_text(OWNER_HELP); return
        if txt in RE_RANDOM_TAG:
            await db.execute("UPDATE config SET random_tag=$1 WHERE id=TRUE", RE_RANDOM_TAG[txt])
            await update.message.reply_text(f"تگ رندوم {'روشن' if RE_RANDOM_TAG[txt] else 'خاموش'} شد."); return
        if txt in RE_GENDER_PROMPT:
            await db.execute("UPDATE config SET gender_prompt=$1 WHERE id=TRUE", RE_GENDER_PROMPT[txt])
            await update.message.reply_text(f"پاپ‌آپ تعیین جنسیت {'روشن' if RE_GENDER_PROMPT[txt] else 'خاموش'} شد."); return

    # ارتقا/عزل (فقط مالک)
    if is_owner(user.id):
        role_map = {
            "ترفیع چت": "chat_admin", "عزل چت": None,
            "ترفیع کال": "call_admin", "عزل کال": None,
            "ترفیع ارشدچت": "senior_chat", "عزل ارشدچت": None,
            "ترفیع ارشدکال": "senior_call", "عزل ارشدکال": None,
            "ترفیع ارشدکل": "senior_all", "عزل ارشدکل": None,
            "ترفیع کانال": "channel_admin", "عزل کانال": None,
        }
        if txt in role_map:
            target = extract_target_from_text_or_reply(update)
            if not target:
                await update.message.reply_text("روی پیام فرد ریپلای کنید یا آیدی عددی بنویسید."); return
            await db.execute("INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING", target)
            await db.execute(
                "UPDATE users SET role=$2, joined_guard_at=COALESCE(joined_guard_at, NOW()) WHERE user_id=$1",
                target, role_map[txt]
            )
            await context.bot.send_message(GUARD_CHAT_ID, f"🔧 {txt} برای {await mention_name(context, target)}", parse_mode=ParseMode.HTML)
            await context.bot.send_message(OWNER_ID, f"🔧 {txt} برای {await mention_name(context, target)}", parse_mode=ParseMode.HTML)
            await update.message.reply_text("انجام شد."); return

    # لیست گارد (مالک و همه نقش‌دارها)
    if txt == "لیست گارد":
        rows = await db.fetch("""
            SELECT user_id, role, rank FROM users
            WHERE role IN ('senior_all','senior_chat','senior_call','channel_admin','chat_admin','call_admin')
            ORDER BY
              CASE role
                WHEN 'senior_all' THEN 0
                WHEN 'senior_chat' THEN 1
                WHEN 'senior_call' THEN 2
                WHEN 'channel_admin' THEN 3
                WHEN 'chat_admin' THEN 4
                WHEN 'call_admin' THEN 5
                ELSE 9 END,
              rank DESC NULLS LAST
        """)
        lines = ["👥 تیم مدیریت:"]
        lines.append(f"- مالک: {await mention_name(context, OWNER_ID)}")
        role_map = {
            "senior_all": "ارشد کل",
            "senior_chat": "ارشد چت",
            "senior_call": "ارشد کال",
            "channel_admin": "ادمین کانال",
            "chat_admin": "ادمین چت",
            "call_admin": "ادمین کال",
        }
        for r in rows:
            rr = role_map.get(r["role"], r["role"])
            lines.append(f"- {rr}: {await mention_name(context, r['user_id'])}")
        await update.message.reply_html("\n".join(lines))
        return

    # ورود/خروج/تغییر فعالیت (مشترک)
    if txt == "ثبت":
        await update.message.reply_text("نوع فعالیت را انتخاب کنید:", reply_markup=kb_checkin()); return

    if txt == "تغییر فعالیت":
        await update.message.reply_text("به چه فعالیتی تغییر کنم؟", reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("تغییر به چت", callback_data="switch_to_chat"),
            InlineKeyboardButton("تغییر به کال", callback_data="switch_to_call"),
        ]])); return

    if txt == "ثبت خروج":
        # محکم: همه‌ی سشن‌های باز هدف بسته شوند
        target = extract_target_from_text_or_reply(update)
        actor_is_mgr = await is_senior_or_owner(user.id)
        uid = target if (actor_is_mgr and target) else user.id
        await end_all_sessions(context, uid, reason=("درخواست مدیر" if uid != user.id else "درخواست متنی"))
        await update.message.reply_text("خروج ثبت شد ✅"); return

    if txt in ("ورود چت","ورود کال"):
        kind = "chat" if txt == "ورود چت" else "call"
        actor_is_mgr = await is_senior_or_owner(user.id)
        target = extract_target_from_text_or_reply(update)
        uid = target if (actor_is_mgr and target) else user.id
        other = "call" if kind=="chat" else "chat"
        old = await get_open_session(uid, other)
        if old: await end_session(context, old["id"], reason="تغییر فعالیت (متنی)")
        await start_session(context, uid, kind)
        txt2 = f"✅ ورود {('چت' if kind=='chat' else 'کال')}: {await mention_name(context, uid)}"
        for dest in (GUARD_CHAT_ID, OWNER_ID):
            try: await context.bot.send_message(dest, txt2, parse_mode=ParseMode.HTML)
            except Exception: pass
        await update.message.reply_text("ثبت شد."); return

    # وضعیت امروزِ شخص
    if txt == "گارد":
        r = await db.fetchrow("SELECT * FROM daily_stats WHERE d=$1 AND user_id=$2", today(), user.id)
        if not r:
            await update.message.reply_text("امروز آماری ندارید."); return
        await update.message.reply_text(
            f"آمار امروز:\n"
            f"پیام‌ها: {r['chat_messages']}\n"
            f"ریپلای زده/دریافت: {r['replies_sent']}/{r['replies_received']}\n"
            f"حضور چت: {human_td(r['chat_seconds'])} | کال: {human_td(r['call_seconds'])} | دفعات کال: {r['call_sessions']}"
        ); return

    # محدود/آزاد رسانه (فقط مالک/ارشد)
    if txt in ("محدود رسانه","آزاد رسانه"):
        is_mgr = await is_senior_or_owner(user.id)
        if not is_mgr and not is_owner(user.id):
            await update.message.reply_text("فقط مالک/ارشد."); return
        target = extract_target_from_text_or_reply(update)
        if not target:
            await update.message.reply_text("روی پیام کاربر ریپلای کنید یا آیدی عددی بنویسید."); return
        try:
            if txt == "محدود رسانه":
                perms = ChatPermissions(
                    can_send_messages=True,
                    can_send_audios=False, can_send_documents=False, can_send_photos=False,
                    can_send_videos=False, can_send_video_notes=False, can_send_voice_notes=False,
                    can_send_polls=False, can_send_other_messages=False, can_add_web_page_previews=False
                )
            else:
                perms = ChatPermissions(
                    can_send_messages=True,
                    can_send_audios=True, can_send_documents=True, can_send_photos=True,
                    can_send_videos=True, can_send_video_notes=True, can_send_voice_notes=True,
                    can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True
                )
            await context.bot.restrict_chat_member(
                MAIN_CHAT_ID, target,
                permissions=perms,
                use_independent_chat_permissions=True
            )
            await update.message.reply_text("انجام شد.")
        except Exception:
            await update.message.reply_text("نیاز به دسترسی ادمین دارم.")
        return

    # سکوت/حذف سکوت (فقط در گروه اصلی و با ریپلای)
    if chat_id == MAIN_CHAT_ID:
        if txt.startswith(("سکوت","خفه")):
            target = extract_target_from_text_or_reply(update)
            if not target:
                await update.message.reply_text("ریپلای لازم است."); return
            perms = ChatPermissions(can_send_messages=False)
            try:
                await context.bot.restrict_chat_member(MAIN_CHAT_ID, target, permissions=perms, use_independent_chat_permissions=True)
                await update.message.reply_text("کاربر در سکوت قرار گرفت.")
            except Exception:
                await update.message.reply_text("نیاز به دسترسی ادمین دارم.")
            return

        if "حذف سکوت" in txt or "حذف خفه" in txt:
            target = extract_target_from_text_or_reply(update)
            if not target:
                await update.message.reply_text("ریپلای لازم است."); return
            perms = ChatPermissions(
                can_send_messages=True,
                can_send_photos=True, can_send_videos=True, can_send_audios=True,
                can_send_documents=True, can_send_polls=True, can_send_video_notes=True, can_send_voice_notes=True,
                can_send_other_messages=True, can_add_web_page_previews=True
            )
            try:
                await context.bot.restrict_chat_member(MAIN_CHAT_ID, target, permissions=perms, use_independent_chat_permissions=True)
                await update.message.reply_text("کاربر از سکوت خارج شد.")
            except Exception:
                await update.message.reply_text("نیاز به دسترسی ادمین دارم.")
            return

    # ثبت جنسیت (خود یا با ریپلای توسط اعضای گارد)
    if txt in ("ثبت پسر","ثبت دختر"):
        target = extract_target_from_text_or_reply(update) or user.id
        if target != user.id and not await is_guard_member(user.id):
            await update.message.reply_text("فقط اعضای گارد می‌توانند برای دیگران ثبت کنند."); return
        await db.execute("INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING", target)
        await db.execute(
            "UPDATE users SET gender=$2 WHERE user_id=$1",
            target, "male" if txt.endswith("پسر") else "female"
        )
        who = "برای خودتان" if target == user.id else f"برای {await mention_name(context, target)}"
        await update.message.reply_html(f"✅ جنسیت {who} ثبت شد.")
        return

    # تگ پسرها/دخترها (و نسخه‌های لیست) — با ریپلای
    if txt in ("تگ پسرها","تگ دخترها","تگ لیست پسر","تگ لیست دختر","تگ لیست همه"):
        if not update.message.reply_to_message:
            await update.message.reply_text("باید روی یک پیام ریپلای کنید."); return
        where = ""
        if txt in ("تگ پسرها","تگ لیست پسر"):
            where = "WHERE gender='male'"
        elif txt in ("تگ دخترها","تگ لیست دختر"):
            where = "WHERE gender='female'"
        rows = await db.fetch(f"SELECT user_id FROM users {where} ORDER BY rank DESC NULLS LAST LIMIT 40")
        if not rows:
            await update.message.reply_text("فهرست خالی است."); return
        mentions = []
        for r in rows:
            mentions.append(await mention_name(context, r["user_id"]))
        # تقسیم اگر خیلی طولانی شد (ساده)
        text = " ".join(mentions)
        await update.message.reply_to_message.reply_html(text)
        return

    # درخواست ادمینی — فقط ارسال گزارش (بدون پاسخ)
    if txt == "درخواست ادمینی":
        uid = user.id
        today_row = await db.fetchrow("SELECT COALESCE(chat_messages,0) msgs, COALESCE(chat_seconds,0) chat_s, COALESCE(call_seconds,0) call_s FROM daily_stats WHERE d=$1 AND user_id=$2", today(), uid)
        since = today() - timedelta(days=7)
        row7 = await db.fetchrow("""
            SELECT COALESCE(SUM(chat_count),0) cnt, MAX(last_active) la
            FROM members_stats WHERE d >= $1 AND user_id=$2
        """, since, uid)
        txtrep = (f"درخواست ادمینی از {await mention_name(context, uid)} (ID <code>{uid}</code>)\n"
                  f"آمار امروز: پیام {today_row['msgs'] if today_row else 0} | چت {human_td((today_row['chat_s'] if today_row else 0))} | کال {human_td((today_row['call_s'] if today_row else 0))}\n"
                  f"آمار ۷ روزه: مجموع پیام‌های چت {row7['cnt'] or 0} | آخرین فعالیت: {row7['la']}")
        for dest in (GUARD_CHAT_ID, OWNER_ID):
            try:
                await context.bot.send_message(dest, txtrep, parse_mode=ParseMode.HTML)
            except Exception:
                pass
        await update.message.reply_text("درخواست شما ثبت و برای مالک/گارد ارسال شد ✅")
        return

    # آمار/آمار کلی/ممنوع/آزاد/زیرنظر/آمار چت الان/کال الان/آمار
    if is_owner(user.id):
        if txt == "آمار چت الان":
            rows = await db.fetch("""
                SELECT u.user_id,u.role, COALESCE(s.chat_messages,0) msgs, COALESCE(s.chat_seconds,0) chat_time
                FROM users u LEFT JOIN daily_stats s ON s.d=$1 AND s.user_id=u.user_id
                WHERE u.role IS NOT NULL
                  AND u.role IN ('senior_all','senior_chat','senior_call','channel_admin','chat_admin','call_admin')
                ORDER BY
                  CASE u.role
                    WHEN 'senior_all' THEN 0
                    WHEN 'senior_chat' THEN 1
                    WHEN 'senior_call' THEN 2
                    WHEN 'channel_admin' THEN 3
                    WHEN 'chat_admin' THEN 4
                    WHEN 'call_admin' THEN 5
                    ELSE 9
                  END, u.rank DESC NULLS LAST
            """, today())
            lines = ["آمار چت تا این لحظه:"]
            for r in rows:
                lines.append(f"{r['role']}: {await mention_name(context, r['user_id'])} | پیام: {r['msgs']} | حضور: {human_td(r['chat_time'])}")
            await update.message.reply_html("\n".join(lines), reply_markup=kb_owner_rate()); return

        if txt == "آمار کال الان":
            rows = await db.fetch("""
                SELECT u.user_id,u.role, COALESCE(s.call_seconds,0) call_time, COALESCE(s.call_sessions,0) calls
                FROM users u LEFT JOIN daily_stats s ON s.d=$1 AND s.user_id=u.user_id
                WHERE u.role IS NOT NULL
                  AND u.role IN ('senior_all','senior_chat','senior_call','channel_admin','chat_admin','call_admin')
                ORDER BY
                  CASE u.role
                    WHEN 'senior_all' THEN 0
                    WHEN 'senior_chat' THEN 1
                    WHEN 'senior_call' THEN 2
                    WHEN 'channel_admin' THEN 3
                    WHEN 'chat_admin' THEN 4
                    WHEN 'call_admin' THEN 5
                    ELSE 9
                  END, u.rank DESC NULLS LAST
            """, today())
            lines = ["آمار کال تا این لحظه:"]
            for r in rows:
                lines.append(f"{r['role']}: {await mention_name(context, r['user_id'])} | زمان کال: {human_td(r['call_time'])} | دفعات: {r['calls']}")
            await update.message.reply_html("\n".join(lines), reply_markup=kb_owner_rate()); return

        if txt == "آمار":
            row = await db.fetchrow("SELECT COUNT(DISTINCT user_id) c FROM members_stats WHERE d=$1 AND chat_count>0", today())
            await update.message.reply_text(f"تعداد کاربران فعال امروز: {row['c']}"); return

        if txt.startswith("آمار کلی کاربر"):
            m = re.search(r"(\d{4,})", txt)
            if not m:
                await update.message.reply_text("آیدی عددی را بنویسید."); return
            uid = int(m.group(1)); since = today() - timedelta(days=30)
            r = await db.fetchrow("""
                SELECT COALESCE(SUM(chat_messages),0) msgs,
                       COALESCE(SUM(replies_sent),0) rs,
                       COALESCE(SUM(replies_received),0) rr,
                       COALESCE(SUM(chat_seconds),0) chat_s,
                       COALESCE(SUM(call_seconds),0) call_s,
                       COALESCE(SUM(call_sessions),0) calls
                FROM daily_stats WHERE d >= $1 AND user_id=$2
            """, since, uid)
            await update.message.reply_text(
                f"آمار ۳۰ روز گذشته {await mention_name(context, uid)}:\n"
                f"- پیام چت: {r['msgs']} (ریپلای زده/دریافت: {r['rs']}/{r['rr']})\n"
                f"- زمان چت: {human_td(r['chat_s'])}\n"
                f"- زمان کال: {human_td(r['call_s'])} | دفعات کال: {r['calls']}"
            , parse_mode=ParseMode.HTML); return

        if txt.startswith("ممنوع") or txt.startswith("آزاد "):
            m = re.search(r"(\d{4,})", txt)
            target = extract_target_from_text_or_reply(update) or (int(m.group(1)) if m else None)
            if not target:
                await update.message.reply_text("روی پیام فرد ریپلای کنید یا آیدی عددی بنویسید."); return
            if txt.startswith("ممنوع"):
                await db.execute("INSERT INTO banned_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", target)
                await update.message.reply_text("در لیست ممنوع اضافه شد.")
            else:
                await db.execute("DELETE FROM banned_users WHERE user_id=$1", target)
                await update.message.reply_text("از لیست ممنوع حذف شد.")
            return

        if txt.startswith("زیرنظر"):
            m = re.search(r"(\d{4,})", txt)
            if not m:
                await update.message.reply_text("آیدی عددی را بنویسید."); return
            uid = int(m.group(1))
            await db.execute("INSERT INTO watchlist(user_id) VALUES($1) ON CONFLICT DO NOTHING", uid)
            await update.message.reply_text("کاربر به لیست زیرنظر افزوده شد."); return

# -------------------- Group message capture --------------------
async def group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return

    msg = update.message
    u = msg.from_user

    await ensure_user(u)
    await bump_member_stats(u.id)

    # جنسیت: اگر نامشخص و ویژگی روشن است → پاپ‌آپ تعیین جنسیت
    conf = await db.fetchrow("SELECT gender_prompt FROM config WHERE id=TRUE")
    if conf and conf["gender_prompt"]:
        g = await db.fetchrow("SELECT gender FROM users WHERE user_id=$1", u.id)
        if not g or not g["gender"]:
            try:
                await msg.reply_text("جنسیتت چیه؟ یکی رو انتخاب کن:", reply_markup=kb_gender(u.id))
            except Exception:
                pass

    # فقط اعضای گارد (و مالک) پاپ‌آپ «ثبت ورود» بگیرند
    is_guard = await is_guard_member(u.id)
    if not is_guard:
        return

    await bump_admin_on_message(msg)

    # اگر سشن باز ندارد → پاپ‌آپ «ثبت ورود»
    open_any = await get_open_session(u.id, None)
    if not open_any:
        try:
            await msg.reply_text("نوع فعالیت را انتخاب کنید:", reply_markup=kb_checkin())
        except Exception:
            pass
        # اطلاع
        text = f"🟢 شروع فعالیت (بدون ثبت ورود): {await mention_name(context, u.id)} — لطفاً ورود چت/کال را انتخاب کند."
        for dest in (GUARD_CHAT_ID, OWNER_ID):
            try:
                await context.bot.send_message(dest, text, parse_mode=ParseMode.HTML)
            except Exception:
                pass
        return
    # اگر نوبت چت باز باشد، خروج خودکار توسط job مدیریت می‌شود

# -------------------- Daily jobs --------------------
async def send_daily_reports(context: ContextTypes.DEFAULT_TYPE):
    d = today() - timedelta(days=1)
    rows = await db.fetch("SELECT * FROM daily_stats WHERE d=$1", d)
    for r in rows:
        uid = r["user_id"]
        txt = (f"گزارش روزانه ({d}):\n"
               f"- پیام‌های چت: {r['chat_messages']}\n"
               f"- ریپلای زده/دریافت: {r['replies_sent']}/{r['replies_received']}\n"
               f"- حضور چت: {human_td(r['chat_seconds'])}\n"
               f"- حضور کال: {human_td(r['call_seconds'])} | دفعات کال: {r['call_sessions']}\n"
               f"- اولین ورود: {r['first_checkin']}\n"
               f"- آخرین خروج: {r['last_checkout']}")
        try: await context.bot.send_message(uid, txt)
        except Exception: pass

    agg = await db.fetch("""
        SELECT u.user_id,u.role, COALESCE(s.chat_messages,0) chat_messages,
               COALESCE(s.call_seconds,0) call_seconds,
               COALESCE(s.chat_seconds,0) chat_seconds
        FROM users u LEFT JOIN daily_stats s ON s.d=$1 AND s.user_id=u.user_id
        WHERE u.role IS NOT NULL
          AND u.role IN ('senior_all','senior_chat','senior_call','channel_admin','chat_admin','call_admin')
        ORDER BY
          CASE u.role
            WHEN 'senior_all' THEN 0
            WHEN 'senior_chat' THEN 1
            WHEN 'senior_call' THEN 2
            WHEN 'channel_admin' THEN 3
            WHEN 'chat_admin' THEN 4
            WHEN 'call_admin' THEN 5
            ELSE 9
          END, u.rank DESC NULLS LAST
    """, d)
    lines = [f"آمار کلی مدیران — {d}"]
    for a in agg:
        lines.append(f"{a['role'] or '-'} | {await mention_name(context, a['user_id'])} | پیام: {a['chat_messages']} | کال: {human_td(a['call_seconds'])} | حضور: {human_td(a['chat_seconds'])}")
    txt = "\n".join(lines)
    for ch in [GUARD_CHAT_ID, OWNER_ID]:
        try: await context.bot.send_message(ch, txt, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("👍 راضی", callback_data="rate_yes"),
            InlineKeyboardButton("👎 ناراضی", callback_data="rate_no")
        ]]))
        except Exception: pass

async def send_candidates_report(context: ContextTypes.DEFAULT_TYPE):
    d = today() - timedelta(days=1)
    rows = await db.fetch(
        "SELECT user_id, chat_count FROM members_stats WHERE d=$1 ORDER BY chat_count DESC LIMIT 10",
        d
    )
    lines = [f"۱۰ کاربر برتر چت ({d})"]
    for i, r in enumerate(rows, start=1):
        lines.append(f"{i}. {await mention_name(context, r['user_id'])} — پیام: {r['chat_count']}")
    try: await context.bot.send_message(OWNER_ID, "\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception: pass

async def send_watchlist_reports(context: ContextTypes.DEFAULT_TYPE):
    d = today() - timedelta(days=1)
    watch = await db.fetch("SELECT user_id FROM watchlist")
    if not watch: return
    for w in watch:
        uid = w["user_id"]
        r = await db.fetchrow("SELECT * FROM daily_stats WHERE d=$1 AND user_id=$2", d, uid)
        if not r: continue
        txt = (f"زیرنظر ({d}) برای {await mention_name(context, uid)}:\n"
               f"- پیام: {r['chat_messages']}, ریپلای ز/د: {r['replies_sent']}/{r['replies_received']}\n"
               f"- حضور چت: {human_td(r['chat_seconds'])}, کال: {human_td(r['call_seconds'])}")
        for ch in [GUARD_CHAT_ID, OWNER_ID]:
            try: await context.bot.send_message(ch, txt, parse_mode=ParseMode.HTML)
            except Exception: pass

async def random_tag_job(context: ContextTypes.DEFAULT_TYPE):
    conf = await db.fetchrow("SELECT random_tag FROM config WHERE id=TRUE")
    if not conf or not conf["random_tag"]: return
    rows = await db.fetch("SELECT user_id FROM members_stats WHERE d=$1 AND chat_count>0 ORDER BY random() LIMIT 1", today())
    if not rows: return
    uid = rows[0]["user_id"]
    phrase = random.choice(FUN_LINES)
    try:
        # منشن با نام
        await context.bot.send_message(MAIN_CHAT_ID, f"{phrase}\n{await mention_name(context, uid)}", parse_mode=ParseMode.HTML)
    except Exception:
        pass

def seconds_until_midnight() -> int:
    n = now(); tomorrow = (n + timedelta(days=1)).date()
    midnight = datetime.combine(tomorrow, datetime.min.time(), tzinfo=TZINFO)
    return max(5, int((midnight - n).total_seconds()))

async def schedule_jobs(app: Application):
    app.job_queue.run_repeating(send_daily_reports, interval=24*3600, first=seconds_until_midnight()+10, name="daily_reports")
    app.job_queue.run_repeating(send_candidates_report, interval=24*3600, first=seconds_until_midnight()+20, name="candidates")
    app.job_queue.run_repeating(send_watchlist_reports, interval=24*3600, first=seconds_until_midnight()+30, name="watchlist")
    app.job_queue.run_repeating(random_tag_job, interval=900, first=300, name="random_tag")

# -------------------- Bootstrapping --------------------
async def post_init(app: Application):
    await db.connect()
    await schedule_jobs(app)
    print("DB connected & jobs scheduled.")

async def post_shutdown(app: Application):
    await db.close()
    print("DB closed.")

def build_app() -> Application:
    app = ApplicationBuilder() \
        .token(BOT_TOKEN) \
        .rate_limiter(AIORateLimiter()) \
        .post_init(post_init) \
        .post_shutdown(post_shutdown) \
        .build()

    # /start
    app.add_handler(CommandHandler("start", start), group=0)

    # Callbacks
    app.add_handler(CallbackQueryHandler(on_contact_btn, pattern="^(contact_guard|contact_owner|back_home|retry_send)$"), group=0)
    app.add_handler(CallbackQueryHandler(on_owner_rate, pattern="^(rate_yes|rate_no)$"), group=0)
    app.add_handler(CallbackQueryHandler(on_checkin_checkout, pattern="^(checkin_chat|checkin_call|checkout_(chat|call)|switch_to_(chat|call))$"), group=0)
    app.add_handler(CallbackQueryHandler(on_my_stats, pattern="^my_stats$"), group=0)
    app.add_handler(CallbackQueryHandler(on_guard_reply_block, pattern="^(reply_|block_)\\d+$"), group=0)
    app.add_handler(CallbackQueryHandler(on_gender_choice, pattern="^gender_(male|female)_\\d+$"), group=0)

    # پیام‌های گارد برای پاسخ ادمین‌ها
    app.add_handler(MessageHandler(filters.Chat(GUARD_CHAT_ID) & ~filters.StatusUpdate.ALL, capture_admin_reply), group=1)

    # پیام‌های گروه اصلی (آمار/پاپ‌آپ ورود/پاپ‌آپ جنسیت)
    app.add_handler(MessageHandler(filters.Chat(MAIN_CHAT_ID) & ~filters.StatusUpdate.ALL, group_message), group=2)

    # فلو تماس در پیوی (کاربر → گارد/مالک)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.StatusUpdate.ALL, pipe_user_message), group=3)

    # دستورات متنی بدون / (در همه‌جا)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_triggers), group=4)

    return app

if __name__ == "__main__":
    application = build_app()
    application.run_polling(drop_pending_updates=True)
