# Souls Security Bot — Single-file (Railway + PostgreSQL)
# PTB v20.x (async)
# ENV: BOT_TOKEN, DATABASE_URL, MAIN_CHAT_ID, GUARD_CHAT_ID, OWNER_ID, TZ(Asia/Tehran)

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

def mention_html(user):
    name = (user.first_name or "") + (" " + user.last_name if user.last_name else "")
    name = name.strip() or (user.username and "@" + user.username) or str(user.id)
    return f'<a href="tg://user?id={user.id}">{name}</a>'

def is_owner(uid: int) -> bool:
    return uid == OWNER_ID

# -------------------- SQL Schema --------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS config (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    auto_mode BOOLEAN NOT NULL DEFAULT FALSE,
    random_tag BOOLEAN NOT NULL DEFAULT FALSE
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
"""

FUN_LINES = [
    "یک لبخند یادت نره!", "انرژی مثبت بفرست ✨", "یه استراحت کوتاه بد نیست!",
    "بچه‌های سولز پشتتن 😉", "حواست به آب خوردن باشه!", "امروز می‌درخشی!",
]
while len(FUN_LINES) < 100:
    FUN_LINES.append(f"پیام فان شماره {len(FUN_LINES)+1}!")

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

def kb_checkout(kind: str):
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ ثبت خروج", callback_data=f"checkout_{kind}")]])

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

def kb_switch():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("تغییر به چت", callback_data="switch_to_chat"),
        InlineKeyboardButton("تغییر به کال", callback_data="switch_to_call"),
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
        # replies
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
        await schedule_inactivity(context, rec["id"])  # 10-minute idle watcher
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
    # clean inline msg if exists
    if sess["open_msg_chat"] and sess["open_msg_id"]:
        try:
            await context.bot.delete_message(sess["open_msg_chat"], sess["open_msg_id"])
        except Exception:
            pass
    # notify guard + owner
    txt = f"{'⛔️' if reason!='manual' else '❌'} خروج {('چت' if sess['kind']=='chat' else 'کال')} — مدت: {human_td(dur)}"
    for ch in [GUARD_CHAT_ID, OWNER_ID]:
        try:
            await context.bot.send_message(ch, txt)
        except Exception:
            pass

async def schedule_inactivity(context: ContextTypes.DEFAULT_TYPE, sess_id: int):
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
    if now() - sess["last_activity_ts"].astimezone(TZINFO) >= timedelta(minutes=10):
        await end_session(context, sid, reason="بدون فعالیت ۱۰ دقیقه")
        context.job.schedule_removal()

# -------------------- Handlers --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update.effective_user)
    if update.message:
        await update.message.reply_html(WELCOME_TEXT, reply_markup=HOME_KB)

async def on_contact_btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data in ("contact_guard","contact_owner"):
        channel = "guard" if q.data.endswith("guard") else "owner"
        context.user_data["contact_channel"] = channel
        await q.message.edit_text(
            f"پیام خود را برای {'گارد مدیران' if channel=='guard' else 'مالک'} ارسال کنید.\nمتن/عکس/ویس مجاز است.",
            reply_markup=kb_back_retry()
        )
    elif q.data == "back_home":
        await q.message.edit_text(WELCOME_TEXT, reply_markup=HOME_KB)
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
        await update.message.reply_text("پیام شما ارسال شد ✅", reply_markup=kb_back_retry())

async def on_guard_reply_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    data = q.data
    if data.startswith("block_"):
        tid = int(data.split("_",1)[1])
        rec = await db.fetchrow("SELECT * FROM contact_threads WHERE id=$1", tid)
        if not rec: return
        await db.execute("INSERT INTO banned_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", rec["user_id"])
        await q.message.reply_text("کاربر مسدود شد.")
    elif data.startswith("reply_"):
        tid = int(data.split("_",1)[1])
        context.chat_data["reply_thread"] = tid
        await q.message.reply_text("پاسخ خود را ارسال کنید.")

async def capture_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid = context.chat_data.get("reply_thread")
    if not tid: return
    rec = await db.fetchrow("SELECT * FROM contact_threads WHERE id=$1", tid)
    if not rec: return
    uid = rec["user_id"]
    try:
        if update.message.text:
            await context.bot.send_message(uid, f"پاسخ مدیریت:\n\n{update.message.text}")
        elif update.message.photo:
            await context.bot.send_photo(uid, update.message.photo[-1].file_id, caption="پاسخ مدیریت:")
        elif update.message.voice:
            await context.bot.send_voice(uid, update.message.voice.file_id, caption="پاسخ مدیریت:")
        else:
            await context.bot.send_message(uid, "پاسخ مدیریت ارسال شد.")
        await update.message.reply_text("پاسخ ارسال شد ✅")
    except Exception:
        await update.message.reply_text("ارسال پاسخ ناموفق بود.")

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

async def on_checkin_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = q.from_user; await ensure_user(u)
    if q.data == "checkin_chat":
        msg = await context.bot.send_message(GUARD_CHAT_ID, f"✅ ورود چت: {mention_html(u)}", parse_mode=ParseMode.HTML, reply_markup=kb_checkout("chat"))
        await start_session(context, u.id, "chat", msg_chat=msg.chat_id, msg_id=msg.message_id)
        await q.message.reply_text("ورود چت ثبت شد.", reply_markup=kb_checkout("chat"))
    elif q.data == "checkin_call":
        msg = await context.bot.send_message(GUARD_CHAT_ID, f"🎧 ورود کال: {mention_html(u)}", parse_mode=ParseMode.HTML, reply_markup=kb_checkout("call"))
        await start_session(context, u.id, "call", msg_chat=msg.chat_id, msg_id=msg.message_id)
        await q.message.reply_text("ورود کال ثبت شد.", reply_markup=kb_checkout("call"))
    elif q.data.startswith("checkout_"):
        kind = q.data.split("_",1)[1]
        sess = await get_open_session(u.id, kind)
        if not sess:
            await q.message.reply_text("جلسه‌ای باز نیست."); return
        await end_session(context, sess["id"], reason="درخواست کاربر")
    elif q.data in ("switch_to_chat","switch_to_call"):
        target = "chat" if q.data.endswith("chat") else "call"
        other = "call" if target=="chat" else "chat"
        old = await get_open_session(u.id, other)
        if old: await end_session(context, old["id"], reason="تغییر فعالیت")
        msg = await context.bot.send_message(GUARD_CHAT_ID, f"🔁 تغییر فعالیت به {('چت' if target=='chat' else 'کال')}: {mention_html(u)}", parse_mode=ParseMode.HTML, reply_markup=kb_checkout(target))
        await start_session(context, u.id, target, msg_chat=msg.chat_id, msg_id=msg.message_id)
        await q.message.reply_text("تغییر فعالیت ثبت شد.", reply_markup=kb_checkout(target))

async def on_my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
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
RE_OWNER_TOGGLE = {"ح غ روشن": True, "ح غ خاموش": False}
RE_RANDOM_TAG = {"تگ رندوم روشن": True, "تگ رندوم خاموش": False}

def extract_target_from_text_or_reply(update: Update):
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        return update.message.reply_to_message.from_user.id
    m = re.search(r"(\d{4,})", update.message.text)
    return int(m.group(1)) if m else None

OWNER_HELP = (
    "راهنمای مالک (دستورات بدون /):\n"
    "• ح غ روشن / ح غ خاموش — ثبت خودکار ورود چت با اولین پیام\n"
    "• تگ رندوم روشن / تگ رندوم خاموش — هر ۱۵ دقیقه یک منشن فان\n"
    "• پینگ — بررسی سرعت پاسخ ربات\n"
    "• ترفیع چت / عزل چت — روی ریپلای یا با آیدی\n"
    "• ترفیع کال / عزل کال — روی ریپلای یا با آیدی\n"
    "• ترفیع ارشدچت / عزل ارشدچت\n"
    "• ترفیع ارشدکال / عزل ارشدکال\n"
    "• ترفیع ارشدکل / عزل ارشدکل\n"
    "• ترفیع کانال / عزل کانال\n"
    "• آمار چت الان / آمار کال الان — تا این لحظه\n"
    "• آمار — تعداد کاربران فعال امروز گروه اصلی\n"
    "• آمار کلی کاربر <آیدی> — گزارش ۳۰ روز گذشته کاربر\n"
    "• ممنوع <آیدی> — اضافه به لیست ممنوع (بن در ورود)\n"
    "• آزاد <آیدی> — حذف از لیست ممنوع\n"
    "• زیرنظر+<آیدی> — گزارش شبانهٔ ویژه به گارد و مالک\n"
)

async def text_triggers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    txt = update.message.text.strip()
    user = update.effective_user
    chat_id = update.effective_chat.id

    # ==== OWNER-ONLY (no slash) ====
    if is_owner(user.id):
        if txt == "راهنما":
            await update.message.reply_text(OWNER_HELP); return
        if txt in RE_OWNER_TOGGLE:
            await db.execute("UPDATE config SET auto_mode=$1 WHERE id=TRUE", RE_OWNER_TOGGLE[txt])
            await update.message.reply_text(f"ح غ {'روشن' if RE_OWNER_TOGGLE[txt] else 'خاموش'} شد."); return
        if txt in RE_RANDOM_TAG:
            await db.execute("UPDATE config SET random_tag=$1 WHERE id=TRUE", RE_RANDOM_TAG[txt])
            await update.message.reply_text(f"تگ رندوم {'روشن' if RE_RANDOM_TAG[txt] else 'خاموش'} شد."); return
        if txt == "پینگ":
            t1 = datetime.now(timezone.utc); m = await update.message.reply_text("پینگ...")
            t2 = datetime.now(timezone.utc)
            await m.edit_text(f"پینگ: {int((t2-t1).total_seconds()*1000)} ms"); return

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
            await context.bot.send_message(GUARD_CHAT_ID, f"🔧 {txt} برای <code>{target}</code>", parse_mode=ParseMode.HTML)
            await context.bot.send_message(OWNER_ID, f"🔧 {txt} برای <code>{target}</code>", parse_mode=ParseMode.HTML)
            await update.message.reply_text("انجام شد."); return

        if txt == "آمار چت الان":
            rows = await db.fetch("""
                SELECT u.user_id,u.role, COALESCE(s.chat_messages,0) msgs, COALESCE(s.chat_seconds,0) chat_time
                FROM users u LEFT JOIN daily_stats s ON s.d=$1 AND s.user_id=u.user_id
                WHERE u.role IS NOT NULL ORDER BY u.role, u.rank DESC NULLS LAST
            """, today())
            lines = ["آمار چت تا این لحظه:"]
            for r in rows:
                lines.append(f"{r['role']}: {r['user_id']} | پیام: {r['msgs']} | حضور: {human_td(r['chat_time'])}")
            await update.message.reply_text("\n".join(lines), reply_markup=kb_owner_rate()); return

        if txt == "آمار کال الان":
            rows = await db.fetch("""
                SELECT u.user_id,u.role, COALESCE(s.call_seconds,0) call_time, COALESCE(s.call_sessions,0) calls
                FROM users u LEFT JOIN daily_stats s ON s.d=$1 AND s.user_id=u.user_id
                WHERE u.role IS NOT NULL ORDER BY u.role, u.rank DESC NULLS LAST
            """, today())
            lines = ["آمار کال تا این لحظه:"]
            for r in rows:
                lines.append(f"{r['role']}: {r['user_id']} | زمان کال: {human_td(r['call_time'])} | دفعات: {r['calls']}")
            await update.message.reply_text("\n".join(lines), reply_markup=kb_owner_rate()); return

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
                f"آمار ۳۰ روز گذشته کاربر {uid}:\n"
                f"- پیام چت: {r['msgs']} (ریپلای زده/دریافت: {r['rs']}/{r['rr']})\n"
                f"- زمان چت: {human_td(r['chat_s'])}\n"
                f"- زمان کال: {human_td(r['call_s'])} | دفعات کال: {r['calls']}"
            ); return

        if txt.startswith("ممنوع"):
            m = re.search(r"(\d{4,})", txt)
            if not m:
                await update.message.reply_text("آیدی عددی را بنویسید."); return
            uid = int(m.group(1))
            await db.execute("INSERT INTO banned_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", uid)
            await update.message.reply_text("در لیست ممنوع اضافه شد."); return

        if txt.startswith("آزاد "):
            m = re.search(r"(\d{4,})", txt)
            if not m:
                await update.message.reply_text("آیدی عددی را بنویسید."); return
            uid = int(m.group(1))
            await db.execute("DELETE FROM banned_users WHERE user_id=$1", uid)
            await update.message.reply_text("از لیست ممنوع حذف شد."); return

        if txt.startswith("زیرنظر"):
            m = re.search(r"(\d{4,})", txt)
            if not m:
                await update.message.reply_text("آیدی عددی را بنویسید."); return
            uid = int(m.group(1))
            await db.execute("INSERT INTO watchlist(user_id) VALUES($1) ON CONFLICT DO NOTHING", uid)
            await update.message.reply_text("کاربر به لیست زیرنظر افزوده شد."); return

    # ==== GENERIC (admins/owner) ====
    if txt == "ثبت":
        await update.message.reply_text("نوع فعالیت را انتخاب کنید:", reply_markup=kb_checkin()); return

    if txt == "تغییر فعالیت":
        await update.message.reply_text("به چه فعالیتی تغییر کنم؟", reply_markup=kb_switch()); return

    if txt in ("ثبت خروج","خروج چت","خروج کال"):
        kind = "chat" if txt != "خروج کال" else "call"
        sess = await get_open_session(user.id, None if txt=="ثبت خروج" else kind)
        if not sess:
            await update.message.reply_text("جلسه‌ای باز نیست."); return
        await end_session(context, sess["id"], reason="درخواست متنی"); return

    if txt in ("ورود چت","ورود کال"):
        kind = "chat" if txt == "ورود چت" else "call"
        msg = await context.bot.send_message(
            GUARD_CHAT_ID,
            f"ورود {('چت' if kind=='chat' else 'کال')}: {mention_html(user)}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_checkout(kind)
        )
        await start_session(context, user.id, kind, msg_chat=msg.chat_id, msg_id=msg.message_id)
        await update.message.reply_text("ثبت شد."); return

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

    # ==== Moderation in MAIN_CHAT ====
    if chat_id == MAIN_CHAT_ID:
        if txt.startswith(("بن","مسدود","ممنوع")):
            target = extract_target_from_text_or_reply(update)
            if not target:
                await update.message.reply_text("ریپلای یا آیدی لازم است."); return
            try:
                await context.bot.ban_chat_member(MAIN_CHAT_ID, target)
            except Exception:
                pass
            await db.execute("INSERT INTO banned_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", target)
            await update.message.reply_text("کاربر مسدود شد."); return

        if txt.startswith(("آزاد","حذف بن","رهایی")):
            target = extract_target_from_text_or_reply(update)
            if not target:
                await update.message.reply_text("ریپلای یا آیدی لازم است."); return
            try:
                await context.bot.unban_chat_member(MAIN_CHAT_ID, target, only_if_banned=True)
            except Exception:
                pass
            await db.execute("DELETE FROM banned_users WHERE user_id=$1", target)
            await update.message.reply_text("کاربر آزاد شد."); return

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

        media_rules = [
            ("بی استیکر", dict(can_send_other_messages=False)),
            ("با استیکر", dict(can_send_other_messages=True)),
            ("بی گیف", dict(can_send_animations=False)),
            ("با گیف", dict(can_send_animations=True)),
            ("بی عکس", dict(can_send_photos=False)),
            ("با عکس", dict(can_send_photos=True)),
            ("بی فیلم", dict(can_send_videos=False)),
            ("با فیلم", dict(can_send_videos=True)),
            ("بی فایل", dict(can_send_documents=False)),
            ("با فایل", dict(can_send_documents=True)),
        ]
        for key, perm in media_rules:
            if txt.startswith(key):
                target = extract_target_from_text_or_reply(update)
                if not target:
                    await update.message.reply_text("ریپلای لازم است."); return
                perms = ChatPermissions(**perm)
                try:
                    await context.bot.restrict_chat_member(MAIN_CHAT_ID, target, permissions=perms, use_independent_chat_permissions=True)
                    await update.message.reply_text("اعمال شد.")
                except Exception:
                    await update.message.reply_text("نیاز به دسترسی ادمین دارم.")
                return

# -------------------- Group message capture --------------------
async def group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MAIN_CHAT_ID:
        return
    msg = update.message; u = msg.from_user
    await ensure_user(u)
    await bump_member_stats(u.id)

    is_admin = await db.fetchrow(
        "SELECT 1 FROM users WHERE user_id=$1 AND (role IS NOT NULL OR $1=$2)", u.id, OWNER_ID
    )
    if is_admin:
        await bump_admin_on_message(msg)
        conf = await db.fetchrow("SELECT auto_mode FROM config WHERE id=TRUE")
        if conf and conf["auto_mode"]:
            sess = await get_open_session(u.id, "chat")
            if not sess:
                m = await context.bot.send_message(
                    GUARD_CHAT_ID,
                    f"✔️ ورود خودکار (چت): {mention_html(u)}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_checkout("chat")
                )
                await start_session(context, u.id, "chat", msg_chat=m.chat_id, msg_id=m.message_id)
            else:
                await db.execute("UPDATE sessions SET last_activity_ts=$1 WHERE id=$2", now(), sess["id"])

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
        try: await context.bot.send_message(ch, txt, reply_markup=kb_owner_rate())
        except Exception: pass

async def send_candidates_report(context: ContextTypes.DEFAULT_TYPE):
    d = today() - timedelta(days=1)
    rows = await db.fetch(
        "SELECT user_id, chat_count FROM members_stats WHERE d=$1 ORDER BY chat_count DESC LIMIT 10",
        d
    )
    lines = [f"۱۰ کاربر برتر چت ({d})"]
    for i, r in enumerate(rows, start=1):
        lines.append(f"{i}. ID {r['user_id']} — پیام: {r['chat_count']}")
    try: await context.bot.send_message(OWNER_ID, "\n".join(lines))
    except Exception: pass

async def send_watchlist_reports(context: ContextTypes.DEFAULT_TYPE):
    d = today() - timedelta(days=1)
    watch = await db.fetch("SELECT user_id FROM watchlist")
    if not watch: return
    for w in watch:
        uid = w["user_id"]
        r = await db.fetchrow("SELECT * FROM daily_stats WHERE d=$1 AND user_id=$2", d, uid)
        if not r: continue
        txt = (f"زیرنظر ({d}) برای {uid}:\n"
               f"- پیام: {r['chat_messages']}, ریپلای ز/د: {r['replies_sent']}/{r['replies_received']}\n"
               f"- حضور چت: {human_td(r['chat_seconds'])}, کال: {human_td(r['call_seconds'])}")
        for ch in [GUARD_CHAT_ID, OWNER_ID]:
            try: await context.bot.send_message(ch, txt)
            except Exception: pass

async def random_tag_job(context: ContextTypes.DEFAULT_TYPE):
    conf = await db.fetchrow("SELECT random_tag FROM config WHERE id=TRUE")
    if not conf or not conf["random_tag"]: return
    rows = await db.fetch("SELECT user_id FROM members_stats WHERE d=$1 AND chat_count>0 ORDER BY random() LIMIT 1", today())
    if not rows: return
    uid = rows[0]["user_id"]
    phrase = random.choice(FUN_LINES)
    try:
        await context.bot.send_message(MAIN_CHAT_ID, f"{phrase}\n<a href=\"tg://user?id={uid}\">‎</a>", parse_mode=ParseMode.HTML)
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

    # /start (CommandHandler — PTB v20)
    app.add_handler(CommandHandler("start", start))

    # Callbacks
    app.add_handler(CallbackQueryHandler(on_contact_btn, pattern="^(contact_guard|contact_owner|back_home|retry_send)$"))
    app.add_handler(CallbackQueryHandler(on_owner_rate, pattern="^(rate_yes|rate_no)$"))
    app.add_handler(CallbackQueryHandler(on_checkin_checkout, pattern="^(checkin_chat|checkin_call|checkout_(chat|call)|switch_to_(chat|call))$"))
    app.add_handler(CallbackQueryHandler(on_my_stats, pattern="^my_stats$"))
    app.add_handler(CallbackQueryHandler(on_guard_reply_block, pattern="^(reply_|block_)\\d+$"))

    # Contact flows
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO | filters.VOICE) & filters.ChatType.PRIVATE, pipe_user_message))
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO | filters.VOICE) & filters.Chat(GUARD_CHAT_ID), capture_admin_reply))

    # Text triggers (no slash)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_triggers))

    # Group capture (main chat)
    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.STICKER | filters.VOICE | filters.VIDEO | filters.ANIMATION) & filters.Chat(MAIN_CHAT_ID),
        group_message
    ))
    return app

if __name__ == "__main__":
    application = build_app()
    application.run_polling(drop_pending_updates=True)
