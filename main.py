
# souls_bot.py
# Single-file Telegram bot for "Souls" team — Railway + PostgreSQL (async)
# Author: ChatGPT (for @imhamedsalehi1998)
#
# ENV VARS expected on Railway:
#   BOT_TOKEN        -> Telegram Bot Token
#   DATABASE_URL     -> PostgreSQL URL (e.g., postgres://user:pass@host:port/dbname)
#   MAIN_CHAT_ID     -> ID of the main group (supergroup) where activity is measured
#   GUARD_CHAT_ID    -> ID of the admins' guard group
#   OWNER_ID         -> Telegram numeric user ID of the owner
#   TZ               -> IANA timezone, default: Asia/Tehran
#
# NOTE on Telegram limitations:
#   The Telegram Bot API does NOT expose reliable "join/leave voice chat" events.
#   This bot tracks CALL activity via explicit check-in/out (متنی یا دکمه).
#   Auto-detection of joining/leaving voice chats is not possible with a normal bot.
#
import asyncio
import os
import re
import random
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

import asyncpg
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatPermissions, MessageEntity, InputMediaPhoto
)
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, ContextTypes, MessageHandler, filters, CallbackQueryHandler,
    AIORateLimiter, ChatMemberHandler, Application, JobQueue
)

# -------------------- Configuration --------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").replace("postgres://", "postgresql://")
MAIN_CHAT_ID = int(os.environ.get("MAIN_CHAT_ID", "0"))
GUARD_CHAT_ID = int(os.environ.get("GUARD_CHAT_ID", "0"))
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
TZ = os.environ.get("TZ", "Asia/Tehran")

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN env var")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL env var")
if not MAIN_CHAT_ID or not GUARD_CHAT_ID or not OWNER_ID:
    print("WARNING: MAIN_CHAT_ID/GUARD_CHAT_ID/OWNER_ID are not fully configured yet.")

TZINFO = ZoneInfo(TZ)

# -------------------- Helpers --------------------

def now() -> datetime:
    return datetime.now(TZINFO)

def today() -> date:
    return now().date()

def human_td(seconds: int) -> str:
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    s2 = s % 60
    parts = []
    if h: parts.append(f"{h}ساعت")
    if m: parts.append(f"{m}دقیقه")
    if s2 and not parts: parts.append(f"{s2}ثانیه")
    return " ".join(parts) or "0"

def mention_html(user):
    name = (user.first_name or "") + (" " + user.last_name if user.last_name else "")
    name = name.strip() or (user.username and "@" + user.username) or str(user.id)
    return f'<a href="tg://user?id={user.id}">{name}</a>'

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

# -------------------- SQL --------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS config (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    auto_mode BOOLEAN NOT NULL DEFAULT FALSE,
    random_tag BOOLEAN NOT NULL DEFAULT FALSE,
    midnight_hour SMALLINT NOT NULL DEFAULT 0
);
INSERT INTO config(id) VALUES (TRUE) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    gender TEXT CHECK (gender IN ('male','female') OR gender IS NULL),
    role TEXT,          -- e.g., 'chat_admin','call_admin','channel_admin','senior_chat','senior_call','senior_all'
    rank INT DEFAULT 0,
    joined_guard_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS banned_users (
    user_id BIGINT PRIMARY KEY
);

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
    open_msg_chat BIGINT
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

CREATE TABLE IF NOT EXISTS watchlist (
    user_id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS contact_threads (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('guard','owner')),
    status TEXT NOT NULL DEFAULT 'open',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_forwarded_msg BIGINT,
    last_forwarded_chat BIGINT
);
"""

FUN_LINES = [
    "یک لیوان آب خوردی امروز؟", "یه لبخند بزن، روزت قشنگ‌تر میشه!",
    "همین الان یه کار خوبِ کوچیک بکن!", "خستگی ناپذیری که!",
    "کافیه یک قدم برداری؛ بقیه‌ش راه میاد.", "بچه‌های سولز پشتتن 😉",
    "یه استراحت کوچولو لازم نیست؟", "سلامتی مهم‌تر از هر چیزه!",
    "از بین ایده‌هات کدومو امروز امتحان می‌کنی؟", "حواست به هیدراته موندن باشه!",
    "یه آهنگ خوب پلی کن!", "انرژی مثبت بفرست برای بقیه!",
    "تو بهترینی ✨", "چقدر خوب که تو اینجایی!",
    "یه پیام مهربون برای یکی بفرست.", "امروزت پربار باشه!",
    "کال بعدی کیه؟ شاید تو! 😄", "بیاین چت رو گرم کنیم!",
    "کسی یه جوک داره؟", "بریم سراغ یه چالش کوچک؟",
    "یه نفس عمیق... آماده‌ای!", "فکر مثبت = نتیجه مثبت.",
    "تا حالا به یه ایده‌ی تازه فکر کردی؟", "امروز با قدرت ادامه بده!",
    "خواب کافی داشتی؟", "یه کش و قوس بده به خودت!", "آب میوه وقتشه؟",
    "دست به کار شو، بقیه راه میاد!", "تو الهام‌بخشی برای بقیه‌ای.",
    "چیز جدید یاد گرفتی امروز؟", "یه خبر خوب بگو!", "یه نفر رو تگ کن و دلگرمش کن!",
    "قدم‌های کوچک، نتیجه‌های بزرگ.", "ذهن آروم، کار دقیق.", "حمایتت می‌کنیم!",
    "پروژه‌ها رو ردیف کن، بزن بریم!", "یه قهوه داغ؟", "یک موزیک شاد پلی کن!",
    "هوای همدیگه رو داریم 🤝", "موفقیت نزدیکه.", "ذهنِ مرتب = عملکرد بهتر.",
    "به خودت افتخار کن!", "یه شکلات کوچیک بخور 🙂", "یه کاری که عقب انداختی رو انجام بده.",
    "امروز با لبخند شروع میشه!", "منشن رندوم: وقت دیده شدنه!",
    "یه قدم برای هدف بزرگت بردار.", "کار تیمی معجزه می‌کنه!", "بهترین خودت باش.",
    "تو می‌تونی!", "امیدوارم روزت عالی باشه!", "یه پیام دوستانه بده به هم‌تیمی‌ها.",
    "نیاز به کمک داری؟ بگو!", "میزان پیشرفتت امروز عالیه!", "عالی پیش می‌ری!",
    "مراقب کمرت باش، صاف بشین! 😅", "به نفس‌هات توجه کن.", "بزن قدش!",
    "یه یادداشت کوچیک برای فردا بنویس.", "کدوم کتاب رو می‌خونی؟", "نوشیدن آب یادت نره.",
    "یه شوخی کوچیک بکن 😄", "منتظر موفقیت‌های بعدیت هستیم.", "تو انگیزه‌بخشی!",
    "یه قدم به جلو بردار.", "یه گفت‌وگوی خوب راه بنداز!", "وقت درخشش توئه ✨",
    "چقدر عالی حرف می‌زنی!", "امروز رو قشنگ‌تر بساز.", "می‌تونی روی ما حساب کنی.",
    "یه عکس از حال و هوات بفرست!", "روزت رنگی رنگی!", "با هم بهتر می‌شیم.",
    "یه ایده جسورانه بگو!", "از خودت مراقبت کن.", "یه تحسین واسه یکی بفرست.",
    "یه نفر رو سورپرایز کن!", "یکم تحرک بد نیست!", "ذهنِ باز = ایده‌های تازه.",
    "بریم به سمت هدف بعدی!", "نظم امروزت عالیه!", "یه کار عقب‌افتاده رو تموم کن.",
    "قدردان زحماتت هستیم.", "یه استیکر باحال بفرست!", "چت رو گرم کن!",
    "مهربونی مسریه 🙂", "تو باعث رشد تیمی!", "روی خودت سرمایه‌گذاری کن.",
    "یه کامنت خوب زیر یه پیام بگذار.", "یه سوال خوب بپرس.", "گام‌هات محکم!",
    "امروزت پر از خبرای خوب!", "یه لبخند به خودت هدیه بده!", "عالی هستی!",
    "یک دقیقه چشم‌هات رو ببند، نفس عمیق.", "کسی به تشویق نیاز داره؟ تو باش!",
    "روی کار مهم تمرکز کن.", "یه پیام الهام‌بخش بفرست.", "امروز بدرخش!"
]
# pad to 100
while len(FUN_LINES) < 100:
    FUN_LINES.append(f"پیام فان شماره {len(FUN_LINES)+1}!")

# --------------- Database utilities ---------------

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

    # Generic helpers
    async def fetchrow(self, q, *args):
        async with self.pool.acquire() as con:
            return await con.fetchrow(q, *args)

    async def fetch(self, q, *args):
        async with self.pool.acquire() as con:
            return await con.fetch(q, *args)

    async def execute(self, q, *args):
        async with self.pool.acquire() as con:
            return await con.execute(q, *args)

db = DB(DATABASE_URL)

# --------------- Core logic ---------------

async def ensure_user(u):
    await db.execute(
        """INSERT INTO users(user_id, first_name, last_name, username)
           VALUES($1,$2,$3,$4)
           ON CONFLICT (user_id) DO UPDATE SET
             first_name=EXCLUDED.first_name,
             last_name=EXCLUDED.last_name,
             username=EXCLUDED.username""",
        u.id, u.first_name, u.last_name, u.username
    )

async def bump_member_stats(uid: int):
    d = today()
    await db.execute(
        """INSERT INTO members_stats(d,user_id,chat_count,last_active)
           VALUES($1,$2,1,$3)
           ON CONFLICT (d,user_id) DO UPDATE SET
             chat_count = members_stats.chat_count + 1,
             last_active = $3""",
        d, uid, now()
    )

async def bump_admin_stats_on_message(message, is_reply: bool):
    uid = message.from_user.id
    d = today()
    # chat message count
    await db.execute(
        """INSERT INTO daily_stats(d,user_id,chat_messages)
           VALUES($1,$2,1)
           ON CONFLICT (d,user_id) DO UPDATE SET chat_messages = daily_stats.chat_messages + 1""",
        d, uid
    )
    # replies sent/received
    if message.reply_to_message and message.reply_to_message.from_user:
        if is_reply:
            await db.execute(
                """INSERT INTO daily_stats(d,user_id,replies_sent)
                   VALUES($1,$2,1)
                   ON CONFLICT (d,user_id) DO UPDATE SET replies_sent = daily_stats.replies_sent + 1""",
                d, uid
            )
        # count reply received for original author if they are tracked user
        orig = message.reply_to_message.from_user.id
        await db.execute(
            """INSERT INTO daily_stats(d,user_id,replies_received)
               VALUES($1,$2,1)
               ON CONFLICT (d,user_id) DO UPDATE SET replies_received = daily_stats.replies_received + 1""",
            d, orig
        )

async def get_open_session(uid: int, kind: str | None = None):
    if kind:
        q = "SELECT * FROM sessions WHERE user_id=$1 AND kind=$2 AND end_ts IS NULL ORDER BY id DESC LIMIT 1"
        return await db.fetchrow(q, uid, kind)
    q = "SELECT * FROM sessions WHERE user_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1"
    return await db.fetchrow(q, uid)

async def start_session(context: ContextTypes.DEFAULT_TYPE, uid: int, kind: str,
                       open_msg_chat: int | None = None, open_msg_id: int | None = None):
    existing = await get_open_session(uid, kind)
    if existing:
        # already open; just refresh last activity
        await db.execute("UPDATE sessions SET last_activity_ts=$1 WHERE id=$2", now(), existing["id"])
        return existing["id"]
    rec = await db.fetchrow(
        """INSERT INTO sessions(user_id,kind,start_ts,last_activity_ts,open_msg_chat,open_msg_id)
           VALUES($1,$2,$3,$3,$4,$5) RETURNING id""",
        uid, kind, now(), open_msg_chat, open_msg_id
    )
    # mark first_checkin if missing
    await db.execute(
        """INSERT INTO daily_stats(d,user_id,first_checkin)
           VALUES($1,$2,$3)
           ON CONFLICT (d,user_id) DO UPDATE SET first_checkin = COALESCE(daily_stats.first_checkin,$3)""",
        today(), uid, now()
    )
    # schedule inactivity checks only for chat
    if kind == "chat":
        await schedule_inactivity_check(context, rec["id"])
    return rec["id"]

async def end_session(context: ContextTypes.DEFAULT_TYPE, sess_id: int, reason: str = "manual"):
    sess = await db.fetchrow("SELECT * FROM sessions WHERE id=$1", sess_id)
    if not sess or sess["end_ts"]:
        return
    end_ts = now()
    await db.execute("UPDATE sessions SET end_ts=$1 WHERE id=$2", end_ts, sess_id)
    delta = int((end_ts - sess["start_ts"]).total_seconds())
    col = "chat_seconds" if sess["kind"] == "chat" else "call_seconds"
    inc_call = ", call_sessions = daily_stats.call_sessions + 1" if sess["kind"] == "call" else ""
    await db.execute(
        f"""INSERT INTO daily_stats(d,user_id,{col},last_checkout)
            VALUES($1,$2,$3,$4)
            ON CONFLICT (d,user_id) DO UPDATE SET
              {col} = daily_stats.{col} + $3,
              last_checkout = $4 {inc_call}""",
        today(), sess["user_id"], delta, end_ts
    )
    # try to delete open inline message if exists
    if sess["open_msg_chat"] and sess["open_msg_id"]:
        try:
            await context.bot.delete_message(chat_id=sess["open_msg_chat"], message_id=sess["open_msg_id"])
        except Exception:
            pass
    # notify guard + owner
    tag = f"<b>خروج {'چت' if sess['kind']=='chat' else 'کال'}</b> برای <code>{sess['user_id']}</code> ⛔️ ({reason}) — مدت: {human_td(delta)}"
    for ch in [GUARD_CHAT_ID, OWNER_ID]:
        try:
            await context.bot.send_message(ch, tag, parse_mode=ParseMode.HTML)
        except Exception:
            pass

async def schedule_inactivity_check(context: ContextTypes.DEFAULT_TYPE, sess_id: int):
    # After 10 minutes of inactivity -> mark exit; then give 3 minutes grace if we previously warned?
    job_name = f"sess_inact_{sess_id}"
    # remove previous job
    for j in context.job_queue.get_jobs_by_name(job_name):
        j.schedule_removal()
    context.job_queue.run_repeating(callback=inactivity_job, interval=60, first=60, name=job_name, data={"sess_id": sess_id})

async def inactivity_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    sess_id = data.get("sess_id")
    sess = await db.fetchrow("SELECT * FROM sessions WHERE id=$1", sess_id)
    if not sess or sess["end_ts"]:
        context.job.schedule_removal()
        return
    # inactivity logic only for chat
    if sess["kind"] != "chat":
        context.job.schedule_removal()
        return
    last_act = sess["last_activity_ts"].astimezone(TZINFO)
    if now() - last_act >= timedelta(minutes=10):
        # auto-end
        await end_session(context, sess_id, reason="بدون فعالیت ۱۰ دقیقه")
        context.job.schedule_removal()

# --------------- Keyboards ---------------

def kb_checkin():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ ورود چت", callback_data="checkin_chat"),
         InlineKeyboardButton("🎧 ورود کال", callback_data="checkin_call")],
    ])

def kb_checkout(kind: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ ثبت خروج", callback_data=f"checkout_{kind}")]
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

# --------------- Handlers ---------------

WELCOME_TEXT = (
    "سلام! این ربات ویژه مالک تیم <b>Souls</b> است.\n"
    "برای ارتباط با گارد مدیران یا مالک از دکمه‌ها استفاده کنید."
)

HOME_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("🛡️ ارتباط با گارد مدیران", callback_data="contact_guard")],
    [InlineKeyboardButton("👤 ارتباط با مالک", callback_data="contact_owner")],
    [InlineKeyboardButton("📊 آمار من", callback_data="my_stats")]
])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    await update.message.reply_html(WELCOME_TEXT, reply_markup=HOME_KB)

# --- Contact flows ---
async def on_contact_btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data in ("contact_guard","contact_owner"):
        channel = "guard" if q.data.endswith("guard") else "owner"
        context.user_data["contact_channel"] = channel
        await q.message.edit_text(
            f"پیام خود را برای {'گارد مدیران' if channel=='guard' else 'مالک'} ارسال کنید.\n"
            "متن/عکس/ویس مجاز است. پس از ارسال، پیام شما منتقل می‌شود.",
            reply_markup=kb_back_retry()
        )
    elif q.data == "back_home":
        await q.message.edit_text(WELCOME_TEXT, reply_markup=HOME_KB)
    elif q.data == "retry_send":
        # no-op, just remind
        await q.answer("پیام جدید ارسال کنید.", show_alert=True)

async def pipe_user_message_to_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    channel = context.user_data.get("contact_channel")
    if not channel:
        return
    user = update.effective_user
    await ensure_user(user)
    # forward content
    caption = f"کاربر: {mention_html(user)}\nID: <code>{user.id}</code>\nیوزرنیم: @{user.username}" if user.username else f"کاربر: {mention_html(user)}\nID: <code>{user.id}</code>"
    dest = GUARD_CHAT_ID if channel=="guard" else OWNER_ID
    sent = None
    try:
        if update.message.photo:
            sent = await context.bot.send_photo(dest, update.message.photo[-1].file_id, caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb_reply_block(0))
        elif update.message.voice:
            sent = await context.bot.send_voice(dest, update.message.voice.file_id, caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb_reply_block(0))
        elif update.message.text:
            sent = await context.bot.send_message(dest, f"{caption}\n\n{update.message.text_html}", parse_mode=ParseMode.HTML, reply_markup=kb_reply_block(0))
        else:
            sent = await context.bot.send_message(dest, f"{caption}\n(نوع رسانه پشتیبانی نشد؛ متن خالی)", parse_mode=ParseMode.HTML, reply_markup=kb_reply_block(0))
    except Exception as e:
        await update.message.reply_text("ارسال به مقصد ناموفق بود.")

    if sent:
        # record thread
        rec = await db.fetchrow(
            "INSERT INTO contact_threads(user_id,channel,last_forwarded_msg,last_forwarded_chat) VALUES($1,$2,$3,$4) RETURNING id",
            user.id, channel, sent.message_id, dest
        )
        # Fix buttons with correct thread id
        try:
            await sent.edit_reply_markup(kb_reply_block(rec["id"]))
        except Exception:
            pass

        await update.message.reply_text("پیام شما ارسال شد ✅", reply_markup=kb_back_retry())

async def on_guard_reply_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data.startswith("block_"):
        thread_id = int(data.split("_",1)[1])
        rec = await db.fetchrow("SELECT * FROM contact_threads WHERE id=$1", thread_id)
        if not rec:
            return
        uid = rec["user_id"]
        # block user: add to banned list
        await db.execute("INSERT INTO banned_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", uid)
        await q.message.reply_text(f"کاربر {uid} مسدود شد.")
    elif data.startswith("reply_"):
        thread_id = int(data.split("_",1)[1])
        context.chat_data["reply_thread"] = thread_id
        await q.message.reply_text("پاسخ خود را بفرستید.")

async def capture_admin_reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # admin in guard replies; send to user
    thread_id = context.chat_data.get("reply_thread")
    if not thread_id:
        return
    rec = await db.fetchrow("SELECT * FROM contact_threads WHERE id=$1", thread_id)
    if not rec:
        await update.message.reply_text("نخ گفتگو پیدا نشد.")
        return
    uid = rec["user_id"]
    try:
        # pipe text-only reply for simplicity
        if update.message.text:
            await context.bot.send_message(uid, f"پاسخ مدیریت:\n\n{update.message.text}")
        elif update.message.voice:
            await context.bot.send_voice(uid, update.message.voice.file_id, caption="پاسخ مدیریت:")
        elif update.message.photo:
            await context.bot.send_photo(uid, update.message.photo[-1].file_id, caption="پاسخ مدیریت:")
        else:
            await context.bot.send_message(uid, "پاسخ مدیریت ارسال شد.")
        await update.message.reply_text("پاسخ ارسال شد ✅\nبرای پاسخ مجدد، پیام دیگری بفرستید.")
    except Exception:
        await update.message.reply_text("ارسال پاسخ به کاربر ناموفق بود.")
    # keep thread open

# --- Owner rating buttons ---
async def on_owner_rate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != OWNER_ID:
        await q.answer("این دکمه فقط برای مالک است.", show_alert=True)
        return
    await q.answer()
    rating = True if q.data.endswith("yes") else False
    await db.execute(
        "INSERT INTO ratings(d,rater_id,rating) VALUES($1,$2,$3) ON CONFLICT (d,rater_id) DO UPDATE SET rating=$3",
        today(), OWNER_ID, rating
    )
    await q.message.reply_text("ثبت شد. سپاس!")

# --- Check-in/out callbacks ---
async def on_checkin_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user = q.from_user
    await ensure_user(user)
    if q.data == "checkin_chat":
        msg = await context.bot.send_message(GUARD_CHAT_ID, f"✅ ورود چت: {mention_html(user)}", parse_mode=ParseMode.HTML,
                                             reply_markup=kb_checkout("chat"))
        sess_id = await start_session(context, user.id, "chat", open_msg_chat=msg.chat_id, open_msg_id=msg.message_id)
        await q.message.reply_text("ورود چت ثبت شد.", reply_markup=kb_checkout("chat"))
    elif q.data == "checkin_call":
        msg = await context.bot.send_message(GUARD_CHAT_ID, f"🎧 ورود کال: {mention_html(user)}", parse_mode=ParseMode.HTML,
                                             reply_markup=kb_checkout("call"))
        sess_id = await start_session(context, user.id, "call", open_msg_chat=msg.chat_id, open_msg_id=msg.message_id)
        await q.message.reply_text("ورود کال ثبت شد.", reply_markup=kb_checkout("call"))
    elif q.data.startswith("checkout_"):
        kind = q.data.split("_",1)[1]
        sess = await get_open_session(user.id, kind)
        if not sess:
            await q.message.reply_text("جلسه‌ای باز نیست.")
            return
        await end_session(context, sess["id"], reason="درخواست کاربر")

# --- Text triggers (no slash) ---
RE_OWNER_TOGGLE = {
    "ح غ روشن": True,
    "ح غ خاموش": False
}
RE_RANDOM_TAG_TOGGLE = {
    "تگ رندوم روشن": True,
    "تگ رندوم خاموش": False
}

async def text_triggers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    txt = update.message.text.strip()
    user = update.effective_user
    chat_id = update.effective_chat.id

    # Ignore blocked users
    b = await db.fetchrow("SELECT 1 FROM banned_users WHERE user_id=$1", user.id)
    if b:
        return

    # Owner toggles
    if is_owner(user.id):
        if txt in RE_OWNER_TOGGLE:
            val = RE_OWNER_TOGGLE[txt]
            await db.execute("UPDATE config SET auto_mode=$1 WHERE id=TRUE", val)
            await update.message.reply_text(f"ح غ {'روشن' if val else 'خاموش'} شد.")
            return
        if txt in RE_RANDOM_TAG_TOGGLE:
            val = RE_RANDOM_TAG_TOGGLE[txt]
            await db.execute("UPDATE config SET random_tag=$1 WHERE id=TRUE", val)
            await update.message.reply_text(f"تگ رندوم {'روشن' if val else 'خاموش'} شد.")
            return
        if txt == "پینگ":
            t1 = datetime.now(timezone.utc)
            m = await update.message.reply_text("پینگ...")
            t2 = datetime.now(timezone.utc)
            ms = int((t2-t1).total_seconds()*1000)
            await m.edit_text(f"پینگ: {ms} ms")
            return

    # Activity commands (all admins/seniors/owner)
    if txt == "ثبت":
        await update.message.reply_text("نوع فعالیت را انتخاب کنید:", reply_markup=kb_checkin())
        return
    if txt in ("ثبت خروج","خروج چت","خروج کال"):
        kind = "chat" if txt != "خروج کال" else "call"
        sess = await get_open_session(user.id, kind if txt!="ثبت خروج" else None)
        if not sess:
            await update.message.reply_text("جلسه‌ای باز نیست.")
        else:
            await end_session(context, sess["id"], reason="درخواست متنی")
        return
    if txt in ("ورود چت","ورود کال"):
        kind = "chat" if txt == "ورود چت" else "call"
        msg = await context.bot.send_message(GUARD_CHAT_ID, f"ورود {('چت' if kind=='chat' else 'کال')}: {mention_html(user)}", parse_mode=ParseMode.HTML, reply_markup=kb_checkout(kind))
        await start_session(context, user.id, kind, open_msg_chat=msg.chat_id, open_msg_id=msg.message_id)
        await update.message.reply_text("ثبت شد.")
        return
    if txt == "گارد":
        d = today()
        row = await db.fetchrow("""SELECT * FROM daily_stats WHERE d=$1 AND user_id=$2""", d, user.id)
        if not row:
            await update.message.reply_text("امروز هنوز آماری ندارید.")
            return
        msg = (
            f"آمار امروز شما:\n"
            f"- پیام‌های چت: {row['chat_messages']}\n"
            f"- ریپلای زده: {row['replies_sent']} | دریافت: {row['replies_received']}\n"
            f"- زمان حضور چت: {human_td(row['chat_seconds'])}\n"
            f"- زمان فعالیت کال: {human_td(row['call_seconds'])}\n"
            f"- دفعات کال: {row['call_sessions']}\n"
        )
        await update.message.reply_text(msg)
        return

    # Moderation (must be in main group)
    if chat_id == MAIN_CHAT_ID and (txt.startswith("بن") or txt.startswith("مسدود") or txt.startswith("ممنوع")):
        target_id = None
        if update.message.reply_to_message:
            target_id = update.message.reply_to_message.from_user.id
        else:
            m = re.search(r"(\d{4,})", txt)
            if m: target_id = int(m.group(1))
        if not target_id:
            await update.message.reply_text("روی پیام کاربر ریپلای کنید یا آیدی عددی را بنویسید.")
            return
        try:
            await context.bot.ban_chat_member(MAIN_CHAT_ID, target_id)
        except Exception:
            pass
        await db.execute("INSERT INTO banned_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", target_id)
        await update.message.reply_text("کاربر مسدود شد.")
        return

    if chat_id == MAIN_CHAT_ID and (txt.startswith("آزاد") or "حذف بن" in txt or "رهایی" in txt):
        target_id = None
        if update.message.reply_to_message:
            target_id = update.message.reply_to_message.from_user.id
        else:
            m = re.search(r"(\d{4,})", txt)
            if m: target_id = int(m.group(1))
        if not target_id:
            await update.message.reply_text("روی پیام کاربر ریپلای کنید یا آیدی عددی را بنویسید.")
            return
        try:
            await context.bot.unban_chat_member(MAIN_CHAT_ID, target_id, only_if_banned=True)
        except Exception:
            pass
        await db.execute("DELETE FROM banned_users WHERE user_id=$1", target_id)
        await update.message.reply_text("کاربر آزاد شد.")
        return

    # Silence / un-silence
    if chat_id == MAIN_CHAT_ID and (txt.startswith("سکوت") or "خفه" in txt):
        target_id = update.message.reply_to_message.from_user.id if update.message.reply_to_message else None
        if not target_id:
            await update.message.reply_text("برای سکوت روی پیام کاربر ریپلای کنید.")
            return
        perms = ChatPermissions(can_send_messages=False)
        try:
            await context.bot.restrict_chat_member(MAIN_CHAT_ID, target_id, permissions=perms, use_independent_chat_permissions=True)
            await update.message.reply_text("کاربر در سکوت قرار گرفت.")
        except Exception:
            await update.message.reply_text("نیاز به دسترسی ادمین دارم.")
        return

    if chat_id == MAIN_CHAT_ID and ("حذف سکوت" in txt or "حذف خفه" in txt):
        target_id = update.message.reply_to_message.from_user.id if update.message.reply_to_message else None
        if not target_id:
            await update.message.reply_text("برای آزادسازی روی پیام کاربر ریپلای کنید.")
            return
        perms = ChatPermissions(
            can_send_messages=True,
            can_send_photos=True, can_send_videos=True, can_send_audios=True,
            can_send_documents=True, can_send_polls=True, can_send_video_notes=True, can_send_voice_notes=True,
            can_send_other_messages=True, can_add_web_page_previews=True
        )
        try:
            await context.bot.restrict_chat_member(MAIN_CHAT_ID, target_id, permissions=perms, use_independent_chat_permissions=True)
            await update.message.reply_text("کاربر از سکوت خارج شد.")
        except Exception:
            await update.message.reply_text("نیاز به دسترسی ادمین دارم.")
        return

    # Gender set
    if txt in ("ثبت پسر","ثبت دختر"):
        gender = "male" if "پسر" in txt else "female"
        await db.execute("UPDATE users SET gender=$2 WHERE user_id=$1", user.id, gender)
        await update.message.reply_text("ثبت شد.")
        return

    # Tag girls/boys
    if txt in ("تگ دخترها","تگ پسرها") and update.message.reply_to_message:
        g = "female" if "دختر" in txt else "male"
        rows = await db.fetch("SELECT user_id FROM users WHERE gender=$1 LIMIT 400", g)
        ids = [r["user_id"] for r in rows]
        if not ids:
            await update.message.reply_text("کسی ثبت نشده.")
            return
        # Telegram limits mentions ~ 30-50 per message; chunk to 20
        chunk = 20
        for i in range(0, len(ids), chunk):
            part = ids[i:i+chunk]
            text = " ".join([f'<a href="tg://user?id={uid}">‎</a>' for uid in part])  # invisible mentions
            try:
                await update.message.reply_to_message.reply_html(text)
            except Exception:
                pass
        return

# --- Capture messages in main group: stats & auto-mode ---
async def group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    msg = update.message
    u = msg.from_user
    await ensure_user(u)
    await bump_member_stats(u.id)

    # If user is admin/senior/owner -> bump daily stats and handle auto-mode
    is_admin = await db.fetchrow("SELECT 1 FROM users WHERE user_id=$1 AND (role IS NOT NULL OR $1=$2)", u.id, OWNER_ID)
    if is_admin:
        await bump_admin_stats_on_message(msg, bool(msg.reply_to_message))
        # refresh session or auto open if auto_mode
        conf = await db.fetchrow("SELECT auto_mode FROM config WHERE id=TRUE")
        if conf and conf["auto_mode"]:
            sess = await get_open_session(u.id, "chat")
            if not sess:
                # auto start
                sent = await context.bot.send_message(GUARD_CHAT_ID, f"✔️ ورود خودکار (چت): {mention_html(u)}", parse_mode=ParseMode.HTML, reply_markup=kb_checkout("chat"))
                sess_id = await start_session(context, u.id, "chat", open_msg_chat=sent.chat_id, open_msg_id=sent.message_id)
            else:
                await db.execute("UPDATE sessions SET last_activity_ts=$1 WHERE id=$2", now(), sess["id"])

# --- Daily jobs ---
async def send_daily_reports(context: ContextTypes.DEFAULT_TYPE):
    d = today() - timedelta(days=1)  # report for the day that just ended
    rows = await db.fetch("SELECT * FROM daily_stats WHERE d=$1 ORDER BY user_id", d)
    if not rows:
        return
    # Per-user
    for r in rows:
        uid = r["user_id"]
        text = (
            f"گزارش روزانه ({d}):\n"
            f"- حضور چت: {human_td(r['chat_seconds'])}\n"
            f"- حضور کال: {human_td(r['call_seconds'])}\n"
            f"- دفعات کال: {r['call_sessions']}\n"
            f"- پیام‌های چت: {r['chat_messages']}\n"
            f"- ریپلای زده/دریافت: {r['replies_sent']}/{r['replies_received']}\n"
            f"- اولین ورود: {r['first_checkin']}\n"
            f"- آخرین خروج: {r['last_checkout']}\n"
        )
        try:
            await context.bot.send_message(uid, text)
        except Exception:
            pass
    # Aggregate to owner & guard
    agg = await db.fetch("""
        SELECT u.user_id,u.role, COALESCE(s.chat_messages,0) chat_messages,
               COALESCE(s.call_seconds,0) call_seconds,
               COALESCE(s.chat_seconds,0) chat_seconds
        FROM users u
        LEFT JOIN daily_stats s ON s.d=$1 AND s.user_id=u.user_id
        WHERE u.role IS NOT NULL
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
        lines.append(f"{a['role'] or '-'} | پیام: {a['chat_messages']} | کال: {human_td(a['call_seconds'])} | حضور: {human_td(a['chat_seconds'])}")
    txt = "\n".join(lines)
    for ch in [GUARD_CHAT_ID, OWNER_ID]:
        try:
            await context.bot.send_message(ch, txt, reply_markup=kb_owner_rate())
        except Exception:
            pass

async def send_candidates_report(context: ContextTypes.DEFAULT_TYPE):
    # Top 10 members by chat (and call if available -> here we only have chat)
    d = today() - timedelta(days=1)
    rows = await db.fetch("""SELECT user_id, chat_count FROM members_stats WHERE d=$1 ORDER BY chat_count DESC LIMIT 10""", d)
    lines = [f"۱۰ کاربر برتر چت ({d})"]
    for i, r in enumerate(rows, start=1):
        lines.append(f"{i}. ID {r['user_id']} — پیام: {r['chat_count']}")
    txt = "\n".join(lines)
    try:
        await context.bot.send_message(OWNER_ID, txt)
    except Exception:
        pass

async def random_tag_job(context: ContextTypes.DEFAULT_TYPE):
    conf = await db.fetchrow("SELECT random_tag FROM config WHERE id=TRUE")
    if not conf or not conf["random_tag"]:
        return
    # pick from members active today
    rows = await db.fetch("""SELECT user_id FROM members_stats WHERE d=$1 AND chat_count>0 ORDER BY random() LIMIT 1""", today())
    if not rows:
        return
    uid = rows[0]["user_id"]
    phrase = random.choice(FUN_LINES)
    try:
        await context.bot.send_message(MAIN_CHAT_ID, f"{phrase}\n{f'<a href=\"tg://user?id={uid}\">‎</a>'}", parse_mode=ParseMode.HTML)
    except Exception:
        pass

# --- Stats on private "آمار من" button ---
async def on_my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    d = today()
    r = await db.fetchrow("SELECT * FROM daily_stats WHERE d=$1 AND user_id=$2", d, uid)
    if not r:
        await q.message.reply_text("امروز آماری ندارید.")
        return
    text = (
        f"آمار امروز:\n"
        f"- حضور چت: {human_td(r['chat_seconds'])}\n"
        f"- حضور کال: {human_td(r['call_seconds'])}\n"
        f"- پیام‌های چت: {r['chat_messages']}\n"
        f"- ریپلای زده/دریافت: {r['replies_sent']}/{r['replies_received']}"
    )
    await q.message.reply_text(text)

# --------------- Midnight scheduler ---------------
def seconds_until_midnight_tz() -> int:
    now_t = now()
    tomorrow = (now_t + timedelta(days=1)).date()
    midnight = datetime.combine(tomorrow, datetime.min.time(), tzinfo=TZINFO)
    return int((midnight - now_t).total_seconds())

async def schedule_jobs(app: Application):
    # Daily reports at 00:00 TZ
    app.job_queue.run_repeating(send_daily_reports, interval=24*3600, first=seconds_until_midnight_tz()+10, name="daily_reports")
    app.job_queue.run_repeating(send_candidates_report, interval=24*3600, first=seconds_until_midnight_tz()+20, name="candidates_report")
    # Random tag every 15 minutes
    app.job_queue.run_repeating(random_tag_job, interval=900, first=300, name="random_tag")

# --------------- Main ---------------
async def main():
    await db.connect()

    app = ApplicationBuilder().token(BOT_TOKEN).rate_limiter(AIORateLimiter()).build()

    # /start
    app.add_handler(MessageHandler(filters.CommandStart(), start))

    # private buttons
    app.add_handler(CallbackQueryHandler(on_contact_btn, pattern="^(contact_guard|contact_owner|back_home|retry_send)$"))
    app.add_handler(CallbackQueryHandler(on_owner_rate, pattern="^(rate_yes|rate_no)$"))
    app.add_handler(CallbackQueryHandler(on_checkin_checkout, pattern="^(checkin_chat|checkin_call|checkout_(chat|call))$"))
    app.add_handler(CallbackQueryHandler(on_my_stats, pattern="^my_stats$"))

    # contact flows
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO | filters.VOICE) & filters.ChatType.PRIVATE, pipe_user_message_to_channel))
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO | filters.VOICE) & filters.Chat(GUARD_CHAT_ID), capture_admin_reply_to_user))

    # text triggers (no slash) — in all chats
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_triggers))

    # group message capture for MAIN_CHAT_ID
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO | filters.STICKER | filters.VOICE | filters.VIDEO | filters.ANIMATION) & filters.Chat(MAIN_CHAT_ID), group_message))

    await schedule_jobs(app)
    print("Souls bot is up.")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    await app.updater.wait_closed()
    await app.stop()
    await app.shutdown()
    await db.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
