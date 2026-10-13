import os
import json
import logging
import asyncio
from aiohttp import web
from datetime import datetime, timezone
from typing import Optional

import aiomysql
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.constants import ChatMemberStatus
from telegram.error import TelegramError, Forbidden, RetryAfter
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("controller-bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]
MYSQL_DATABASE = os.environ["MYSQL_DATABASE"]
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
# Optional required public channel, e.g. @yourchannel. Bot must be able to check membership.
FORCE_SUB_CHANNEL = os.getenv("FORCE_SUB_CHANNEL", "").strip()
FORCE_SUB_URL = os.getenv("FORCE_SUB_URL", "").strip()
TIMEZONE_NAME = os.getenv("TIMEZONE", "UTC")

pool: Optional[aiomysql.Pool] = None
# Per-user temporary composer state. Durable jobs/posts live in MySQL.
drafts = {}
support_waiting = set()
replying_to = {}

MENU = [
    [KeyboardButton("📝 Create Post"), KeyboardButton("📣 Broadcast")],
    [KeyboardButton("📡 My Channels"), KeyboardButton("⏰ Scheduled Posts")],
    [KeyboardButton("🆘 Support")]
]
ADMIN_MENU = MENU + [[KeyboardButton("👑 Admin Panel")]]


async def db():
    global pool
    if pool is None:
        pool = await aiomysql.create_pool(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
            password=MYSQL_PASSWORD, db=MYSQL_DATABASE,
            minsize=1, maxsize=8, autocommit=True,
            charset="utf8mb4", connect_timeout=10
        )
    return pool


async def init_db():
    p = await db()
    ddl = [
        """CREATE TABLE IF NOT EXISTS controller_channels (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            owner_id BIGINT NOT NULL,
            chat_id BIGINT NOT NULL,
            title VARCHAR(255) NOT NULL DEFAULT '',
            username VARCHAR(255) NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_controller_chat_owner (owner_id, chat_id),
            KEY idx_cc_chat (chat_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        """CREATE TABLE IF NOT EXISTS scheduled_posts (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            owner_id BIGINT NOT NULL,
            chat_id BIGINT NOT NULL,
            content_type VARCHAR(20) NOT NULL,
            text_content MEDIUMTEXT NULL,
            file_id TEXT NULL,
            buttons_json JSON NULL,
            publish_at DATETIME NOT NULL,
            status ENUM('pending','processing','published','failed','cancelled') NOT NULL DEFAULT 'pending',
            error_text TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            published_at DATETIME NULL,
            KEY idx_schedule_due (status, publish_at),
            KEY idx_schedule_owner (owner_id, status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        """CREATE TABLE IF NOT EXISTS banned_users (
            user_id BIGINT NOT NULL PRIMARY KEY,
            reason VARCHAR(500) NULL,
            banned_by BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        """CREATE TABLE IF NOT EXISTS controller_support (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id BIGINT NOT NULL,
            admin_id BIGINT NOT NULL,
            question MEDIUMTEXT NOT NULL,
            answer MEDIUMTEXT NULL,
            status ENUM('open','answered') NOT NULL DEFAULT 'open',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            answered_at DATETIME NULL,
            KEY idx_support_status (status, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        """CREATE TABLE IF NOT EXISTS controller_broadcasts (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            admin_id BIGINT NOT NULL,
            target ENUM('users','channels','both') NOT NULL,
            content_type VARCHAR(20) NOT NULL,
            text_content MEDIUMTEXT NULL,
            file_id TEXT NULL,
            status ENUM('queued','running','completed','failed') NOT NULL DEFAULT 'queued',
            sent_count INT NOT NULL DEFAULT 0,
            failed_count INT NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at DATETIME NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        """CREATE TABLE IF NOT EXISTS controller_broadcast_targets (
            broadcast_id BIGINT UNSIGNED NOT NULL,
            target_id BIGINT NOT NULL,
            target_type ENUM('user','channel') NOT NULL,
            status ENUM('pending','sent','failed') NOT NULL DEFAULT 'pending',
            error_text VARCHAR(500) NULL,
            PRIMARY KEY (broadcast_id, target_id, target_type),
            KEY idx_bct_status (broadcast_id, status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        """CREATE TABLE IF NOT EXISTS controller_users (
            user_id BIGINT NOT NULL PRIMARY KEY,
            first_name VARCHAR(255) NOT NULL DEFAULT '',
            username VARCHAR(255) NULL,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        """CREATE TABLE IF NOT EXISTS controller_user_states (
            user_id BIGINT NOT NULL PRIMARY KEY,
            state VARCHAR(80) NOT NULL,
            payload JSON NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
    ]
    async with p.acquire() as conn:
        async with conn.cursor() as cur:
            for sql in ddl:
                await cur.execute(sql)
    log.info("Additive tables verified; existing tables were not altered or dropped.")


async def q(sql, args=(), fetch=False, many=False):
    p = await db()
    async with p.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, args)
            if fetch:
                return await cur.fetchall() if many else await cur.fetchone()
            return cur.lastrowid


async def save_user(user):
    # Dedicated controller table avoids assuming the unknown schema of existing `users`.
    await q(
        """INSERT INTO controller_users(user_id,first_name,username)
           VALUES(%s,%s,%s)
           ON DUPLICATE KEY UPDATE first_name=VALUES(first_name), username=VALUES(username),
           last_seen=CURRENT_TIMESTAMP""",
        (user.id, user.first_name or "", user.username)
    )
    # Best-effort additive write to legacy `users` only if recognized columns exist.
    # Never ALTER, DELETE, TRUNCATE, or overwrite unknown legacy columns.
    try:
        cols = await q(
            """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA=%s AND TABLE_NAME='users'""",
            (MYSQL_DATABASE,), fetch=True, many=True
        )
        names = {r["COLUMN_NAME"] for r in cols or []}
        mapping = {
            "user_id": user.id, "telegram_id": user.id,
            "first_name": user.first_name or "", "name": user.first_name or "",
            "username": user.username, "telegram_username": user.username
        }
        data = {k: v for k, v in mapping.items() if k in names}
        # Avoid inserting into a legacy table if we cannot identify a likely Telegram ID column.
        id_col = next((c for c in ("user_id", "telegram_id") if c in data), None)
        if id_col and data:
            cols_sql = ",".join(f"`{c}`" for c in data)
            placeholders = ",".join(["%s"] * len(data))
            updates = [c for c in data if c != id_col]
            sql = f"INSERT INTO `users` ({cols_sql}) VALUES ({placeholders})"
            if updates:
                sql += " ON DUPLICATE KEY UPDATE " + ",".join(f"`{c}`=VALUES(`{c}`)" for c in updates)
            await q(sql, tuple(data.values()))
    except Exception as e:
        # Existing schema may be incompatible; dedicated table remains authoritative.
        log.warning("Legacy users table was not written (schema compatibility): %s", e)


async def is_banned(user_id):
    return bool(await q("SELECT user_id FROM banned_users WHERE user_id=%s", (user_id,), fetch=True))


async def force_sub_ok(context, user_id):
    if not FORCE_SUB_CHANNEL:
        return True
    try:
        member = await context.bot.get_chat_member(FORCE_SUB_CHANNEL, user_id)
        return member.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except TelegramError:
        log.warning("Force-sub membership check failed; channel=%s", FORCE_SUB_CHANNEL)
        return False


def main_keyboard(user_id):
    return ReplyKeyboardMarkup(ADMIN_MENU if user_id == ADMIN_ID else MENU, resize_keyboard=True)


def force_sub_markup():
    rows = []
    if FORCE_SUB_URL:
        rows.append([InlineKeyboardButton("📢 Join Required Channel", url=FORCE_SUB_URL)])
    rows.append([InlineKeyboardButton("✅ I Joined — Check Again", callback_data="check_sub")])
    return InlineKeyboardMarkup(rows)


async def guard(update, context):
    user = update.effective_user
    if not user:
        return False
    await save_user(user)
    if await is_banned(user.id):
        msg = update.effective_message
        if msg:
            await msg.reply_text("⛔ আপনার অ্যাকাউন্ট বট ব্যবহারের জন্য নিষিদ্ধ।")
        return False
    if user.id != ADMIN_ID and not await force_sub_ok(context, user.id):
        msg = update.effective_message
        if msg:
            await msg.reply_text(
                "🔒 বট ব্যবহার করতে আগে আমাদের চ্যানেলে Join করুন, তারপর নিচের বাটনে চাপ দিন।",
                reply_markup=force_sub_markup()
            )
        return False
    return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    is_new = not await q("SELECT user_id FROM controller_users WHERE user_id=%s", (user.id,), fetch=True)
    await save_user(user)
    if await is_banned(user.id):
        await update.effective_message.reply_text("⛔ আপনার অ্যাকাউন্ট বট ব্যবহারের জন্য নিষিদ্ধ।")
        return
    if user.id != ADMIN_ID and not await force_sub_ok(context, user.id):
        await update.effective_message.reply_text(
            "🔒 বট ব্যবহার করার আগে নির্দিষ্ট চ্যানেলে Join করুন।",
            reply_markup=force_sub_markup()
        )
        return
    await update.effective_message.reply_text(
        f"স্বাগতম, {user.first_name}! 👋\n\n"
        "এই Controller Bot দিয়ে আপনার চ্যানেল ম্যানেজ, পোস্ট তৈরি, বাটন যোগ এবং পোস্ট শিডিউল করতে পারবেন।",
        reply_markup=main_keyboard(user.id)
    )
    if is_new:
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"🆕 নতুন ইউজার বট Start করেছে\n"
                f"Name: {user.first_name}\nUsername: @{user.username or 'N/A'}\nID: `{user.id}`",
                parse_mode="Markdown"
            )
        except TelegramError:
            log.exception("Admin notification failed")


async def check_sub_callback(update, context):
    query = update.callback_query
    await query.answer()
    if await force_sub_ok(context, query.from_user.id):
        await query.message.reply_text("✅ Membership verified. এখন বট ব্যবহার করতে পারবেন।",
                                       reply_markup=main_keyboard(query.from_user.id))
    else:
        await query.answer("চ্যানেলে Join করে আবার চেষ্টা করুন।", show_alert=True)


async def addchannel(update, context):
    if not await guard(update, context):
        return
    if not context.args:
        await update.effective_message.reply_text(
            "ব্যবহার: /addchannel @channelusername\nবটকে ওই চ্যানেলে Admin করতে হবে।"
        )
        return
    target = context.args[0]
    try:
        chat = await context.bot.get_chat(target)
        member = await context.bot.get_chat_member(chat.id, context.bot.id)
        if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            await update.effective_message.reply_text("❌ আগে বটকে চ্যানেলের Admin করুন।")
            return
        await q(
            """INSERT INTO controller_channels(owner_id,chat_id,title,username)
               VALUES(%s,%s,%s,%s)
               ON DUPLICATE KEY UPDATE title=VALUES(title), username=VALUES(username)""",
            (update.effective_user.id, chat.id, chat.title or str(chat.id), chat.username)
        )
        await update.effective_message.reply_text(f"✅ চ্যানেল যুক্ত হয়েছে: {chat.title}\nID: {chat.id}")
    except TelegramError as e:
        await update.effective_message.reply_text(f"চ্যানেল যোগ করা যায়নি: {e}")


async def my_channels(update, context):
    if not await guard(update, context):
        return
    rows = await q("SELECT * FROM controller_channels WHERE owner_id=%s ORDER BY id DESC",
                   (update.effective_user.id,), fetch=True, many=True)
    if not rows:
        await update.effective_message.reply_text("কোনো চ্যানেল যোগ করা নেই।\n/addchannel @channelusername")
        return
    buttons = [[InlineKeyboardButton(f"📡 {r['title']}", callback_data=f"pick:{r['chat_id']}")]
               for r in rows]
    await update.effective_message.reply_text("পোস্ট করার জন্য চ্যানেল নির্বাচন করুন:", reply_markup=InlineKeyboardMarkup(buttons))


async def create_post(update, context):
    if not await guard(update, context):
        return
    rows = await q("SELECT * FROM controller_channels WHERE owner_id=%s", (update.effective_user.id,), fetch=True, many=True)
    if not rows:
        await update.effective_message.reply_text("আগে /addchannel @channelusername দিয়ে চ্যানেল যোগ করুন।")
        return
    buttons = [[InlineKeyboardButton(r["title"], callback_data=f"compose:{r['chat_id']}")] for r in rows]
    await update.effective_message.reply_text("কোন চ্যানেলে পোস্ট তৈরি করবেন?", reply_markup=InlineKeyboardMarkup(buttons))


async def callback(update, context):
    query = update.callback_query
    data = query.data or ""
    await query.answer()
    uid = query.from_user.id
    if data == "check_sub":
        return await check_sub_callback(update, context)
    if data.startswith("compose:"):
        chat_id = int(data.split(":", 1)[1])
        row = await q("SELECT id FROM controller_channels WHERE owner_id=%s AND chat_id=%s", (uid, chat_id), fetch=True)
        if not row:
            await query.message.reply_text("এই চ্যানেল আপনার অ্যাকাউন্টে নেই।")
            return
        drafts[uid] = {"chat_id": chat_id, "content_type": "text", "text": None, "file_id": None, "buttons": []}
        await query.message.reply_text(
            "এখন পোস্টের Text পাঠান, অথবা ছবি/ভিডিও পাঠান।\n"
            "পোস্টে URL বাটন যোগ করতে পরে /addbutton Label https://example.com ব্যবহার করুন।\n"
            "Publish: /publish | Schedule: /schedule YYYY-MM-DD HH:MM"
        )
    elif data.startswith("pick:"):
        chat_id = int(data.split(":", 1)[1])
        row = await q("SELECT title FROM controller_channels WHERE owner_id=%s AND chat_id=%s", (uid, chat_id), fetch=True)
        if row:
            await query.message.reply_text(f"নির্বাচিত: {row['title']}\nপোস্ট তৈরির জন্য 📝 Create Post চাপুন।")
    elif data.startswith("ban:") and uid == ADMIN_ID:
        target = int(data.split(":", 1)[1])
        await q("INSERT IGNORE INTO banned_users(user_id,reason,banned_by) VALUES(%s,%s,%s)",
                (target, "Admin action", ADMIN_ID))
        await query.message.reply_text(f"⛔ User {target} banned.")
    elif data.startswith("unban:") and uid == ADMIN_ID:
        target = int(data.split(":", 1)[1])
        await q("DELETE FROM banned_users WHERE user_id=%s", (target,))
        await query.message.reply_text(f"✅ User {target} unbanned.")
    elif data.startswith("reply:") and uid == ADMIN_ID:
        target = int(data.split(":", 1)[1])
        replying_to[uid] = target
        await query.message.reply_text(f"এই ইউজারকে রিপ্লাই লিখুন: {target}")
    elif data.startswith("broadcast:") and uid == ADMIN_ID:
        target = data.split(":", 1)[1]
        drafts[uid] = {"mode": "broadcast", "target": target}
        await query.message.reply_text("এখন ব্রডকাস্টের Text অথবা Photo with caption পাঠান।")


async def add_button(update, context):
    if not await guard(update, context):
        return
    d = drafts.get(update.effective_user.id)
    if not d:
        await update.effective_message.reply_text("আগে 📝 Create Post দিয়ে চ্যানেল নির্বাচন করুন।")
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text("ব্যবহার: /addbutton Button_Name https://example.com")
        return
    url = context.args[-1]
    label = " ".join(context.args[:-1])
    if not url.startswith(("https://", "http://")):
        await update.effective_message.reply_text("URL অবশ্যই http:// অথবা https:// দিয়ে শুরু হবে।")
        return
    d["buttons"].append({"text": label[:64], "url": url})
    await update.effective_message.reply_text(f"✅ বাটন যোগ হয়েছে: {label}\nআরও বাটন যোগ করতে আবার /addbutton দিন।")


async def capture_content(update, context):
    user = update.effective_user
    if not user:
        return
    if user.id == ADMIN_ID and user.id in replying_to:
        target = replying_to.pop(user.id)
        try:
            if update.effective_message.photo:
                await context.bot.send_photo(target, update.effective_message.photo[-1].file_id,
                                             caption=update.effective_message.caption or "")
            elif update.effective_message.video:
                await context.bot.send_video(target, update.effective_message.video.file_id,
                                             caption=update.effective_message.caption or "")
            else:
                await context.bot.send_message(target, update.effective_message.text or "")
            await update.effective_message.reply_text("✅ Reply sent.")
        except TelegramError as e:
            await update.effective_message.reply_text(f"Reply failed: {e}")
        return
    if not await guard(update, context):
        return
    d = drafts.get(user.id)
    if d and d.get("mode") == "broadcast" and user.id == ADMIN_ID:
        typ = "photo" if update.effective_message.photo else "text"
        file_id = update.effective_message.photo[-1].file_id if update.effective_message.photo else None
        text_content = update.effective_message.caption if file_id else update.effective_message.text
        await create_broadcast(update, context, d["target"], typ, text_content, file_id)
        drafts.pop(user.id, None)
        return
    if d:
        msg = update.effective_message
        if msg.photo:
            d["content_type"] = "photo"; d["file_id"] = msg.photo[-1].file_id; d["text"] = msg.caption or ""
        elif msg.video:
            d["content_type"] = "video"; d["file_id"] = msg.video.file_id; d["text"] = msg.caption or ""
        elif msg.text:
            d["content_type"] = "text"; d["text"] = msg.text
        else:
            await msg.reply_text("Text, photo অথবা video পাঠান।"); return
        await msg.reply_text(
            "✅ Draft saved in memory.\n"
            f"Buttons: {len(d['buttons'])}\n"
            "Publish করতে /publish অথবা শিডিউল করতে /schedule YYYY-MM-DD HH:MM দিন।"
        )
        return
    if user.id in support_waiting:
        support_waiting.discard(user.id)
        msg = update.effective_message
        content = msg.text or msg.caption or "[media]"
        sid = await q(
            "INSERT INTO controller_support(user_id,admin_id,question) VALUES(%s,%s,%s)",
            (user.id, ADMIN_ID, content)
        )
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"🆘 Support #{sid}\nUser: {user.first_name} (@{user.username or 'N/A'})\nID: {user.id}\n\n{content}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Reply", callback_data=f"reply:{user.id}"),
                                                     InlineKeyboardButton("⛔ Ban", callback_data=f"ban:{user.id}")]])
            )
            await msg.reply_text("✅ আপনার মেসেজ অ্যাডমিনের কাছে পাঠানো হয়েছে।")
        except TelegramError:
            await msg.reply_text("মেসেজ পাঠানো যায়নি। পরে আবার চেষ্টা করুন।")
        return


def post_markup(buttons):
    if not buttons:
        return None
    # Simple 2-column grid layout.
    rows = []
    for i in range(0, len(buttons), 2):
        rows.append([InlineKeyboardButton(b["text"], url=b["url"]) for b in buttons[i:i+2]])
    return InlineKeyboardMarkup(rows)


async def publish_draft(update, context):
    if not await guard(update, context):
        return
    d = drafts.get(update.effective_user.id)
    if not d or "chat_id" not in d or not d.get("text") and not d.get("file_id"):
        await update.effective_message.reply_text("কোনো সম্পূর্ণ draft নেই। 📝 Create Post চাপুন।")
        return
    try:
        await send_post(context, d["chat_id"], d["content_type"], d.get("text"), d.get("file_id"), d.get("buttons", []))
        drafts.pop(update.effective_user.id, None)
        await update.effective_message.reply_text("✅ পোস্ট সফলভাবে প্রকাশিত হয়েছে।")
    except TelegramError as e:
        await update.effective_message.reply_text(f"পোস্ট করা যায়নি: {e}")


async def schedule_draft(update, context):
    if not await guard(update, context):
        return
    d = drafts.get(update.effective_user.id)
    if not d or "chat_id" not in d or not d.get("text") and not d.get("file_id"):
        await update.effective_message.reply_text("আগে একটি পোস্ট draft তৈরি করুন।")
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text("ব্যবহার: /schedule 2026-12-31 20:30 (server timezone UTC; set TIMEZONE)")
        return
    try:
        dt = datetime.strptime(context.args[0] + " " + context.args[1], "%Y-%m-%d %H:%M")
        if dt <= datetime.now():
            await update.effective_message.reply_text("ভবিষ্যতের সময় দিন।")
            return
        await q(
            """INSERT INTO scheduled_posts(owner_id,chat_id,content_type,text_content,file_id,buttons_json,publish_at)
               VALUES(%s,%s,%s,%s,%s,%s,%s)""",
            (update.effective_user.id, d["chat_id"], d["content_type"], d.get("text"), d.get("file_id"),
             json.dumps(d.get("buttons", [])), dt)
        )
        drafts.pop(update.effective_user.id, None)
        await update.effective_message.reply_text(f"⏰ পোস্ট শিডিউল হয়েছে: {dt:%Y-%m-%d %H:%M}")
    except ValueError:
        await update.effective_message.reply_text("তারিখের ফরম্যাট ভুল। উদাহরণ: /schedule 2026-12-31 20:30")


async def send_post(context, chat_id, content_type, text_content, file_id, buttons):
    markup = post_markup(buttons or [])
    if content_type == "photo":
        await context.bot.send_photo(chat_id, file_id, caption=text_content or None, reply_markup=markup)
    elif content_type == "video":
        await context.bot.send_video(chat_id, file_id, caption=text_content or None, reply_markup=markup)
    else:
        await context.bot.send_message(chat_id, text_content or "", reply_markup=markup, disable_web_page_preview=False)


async def scheduler_job(context):
    # Atomic status claim prevents two workers publishing the same due row.
    rows = await q(
        """SELECT * FROM scheduled_posts WHERE status='pending' AND publish_at<=UTC_TIMESTAMP()
           ORDER BY publish_at ASC LIMIT 10""", fetch=True, many=True
    )
    for row in rows or []:
        changed = await q(
            "UPDATE scheduled_posts SET status='processing' WHERE id=%s AND status='pending'",
            (row["id"],)
        )
        # aiomysql autocommit UPDATE rowcount isn't returned by helper; recheck ownership.
        check = await q("SELECT status FROM scheduled_posts WHERE id=%s", (row["id"],), fetch=True)
        if not check or check["status"] != "processing":
            continue
        try:
            buttons = json.loads(row["buttons_json"] or "[]")
            await send_post(context, row["chat_id"], row["content_type"], row["text_content"], row["file_id"], buttons)
            await q("UPDATE scheduled_posts SET status='published',published_at=UTC_TIMESTAMP() WHERE id=%s", (row["id"],))
        except Exception as e:
            await q("UPDATE scheduled_posts SET status='failed',error_text=%s WHERE id=%s", (str(e)[:1000], row["id"]))
            log.exception("Scheduled post %s failed", row["id"])


async def scheduled_list(update, context):
    if not await guard(update, context):
        return
    rows = await q(
        "SELECT id,chat_id,publish_at,status FROM scheduled_posts WHERE owner_id=%s ORDER BY publish_at DESC LIMIT 20",
        (update.effective_user.id,), fetch=True, many=True
    )
    if not rows:
        await update.effective_message.reply_text("কোনো scheduled post নেই।")
        return
    text = "\n".join(f"#{r['id']} | {r['publish_at']} | {r['status']} | chat {r['chat_id']}" for r in rows)
    await update.effective_message.reply_text("⏰ Scheduled posts:\n" + text)


async def support(update, context):
    if not await guard(update, context):
        return
    support_waiting.add(update.effective_user.id)
    await update.effective_message.reply_text("আপনার প্রশ্ন বা সমস্যাটি লিখে পাঠান।")


async def admin_panel(update, context):
    if update.effective_user.id != ADMIN_ID:
        await update.effective_message.reply_text("⛔ Admin only.")
        return
    await update.effective_message.reply_text(
        "👑 Admin Panel",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Analytics", callback_data="admin:stats")],
            [InlineKeyboardButton("👥 Users", callback_data="admin:users"),
             InlineKeyboardButton("📁 Channels", callback_data="admin:channels")],
            [InlineKeyboardButton("📢 Broadcast Users", callback_data="broadcast:users")],
            [InlineKeyboardButton("📢 Broadcast Channels", callback_data="broadcast:channels")],
            [InlineKeyboardButton("📢 Broadcast Both", callback_data="broadcast:both")],
            [InlineKeyboardButton("⛔ Ban User", callback_data="admin:ban"),
             InlineKeyboardButton("✅ Unban User", callback_data="admin:unban")]
        ])
    )


async def admin_callbacks(update, context):
    query = update.callback_query
    data = query.data or ""
    if not data.startswith("admin:"):
        return
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.message.reply_text("⛔ Admin only."); return
    action = data.split(":", 1)[1]
    if action == "stats":
        users = await q("SELECT COUNT(*) c FROM controller_users", fetch=True)
        channels = await q("SELECT COUNT(*) c FROM controller_channels", fetch=True)
        scheduled = await q("SELECT COUNT(*) c FROM scheduled_posts WHERE status='pending'", fetch=True)
        banned = await q("SELECT COUNT(*) c FROM banned_users", fetch=True)
        await query.message.reply_text(
            f"📊 Analytics\nUsers: {users['c']}\nChannels: {channels['c']}\n"
            f"Pending scheduled posts: {scheduled['c']}\nBanned users: {banned['c']}"
        )
    elif action == "users":
        rows = await q("SELECT user_id,first_name,username FROM controller_users ORDER BY created_at DESC LIMIT 30",
                       fetch=True, many=True)
        if not rows: await query.message.reply_text("No users."); return
        for r in rows:
            await query.message.reply_text(
                f"{r['first_name']} (@{r['username'] or 'N/A'}) — {r['user_id']}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⛔ Ban", callback_data=f"ban:{r['user_id']}"),
                    InlineKeyboardButton("↩️ Reply", callback_data=f"reply:{r['user_id']}")
                ]])
            )
    elif action == "channels":
        rows = await q("SELECT owner_id,chat_id,title,username FROM controller_channels ORDER BY id DESC LIMIT 50",
                       fetch=True, many=True)
        await query.message.reply_text("\n".join(
            f"{r['title']} | {r['chat_id']} | owner {r['owner_id']}" for r in rows
        ) or "No channels.")
    elif action in ("ban", "unban"):
        drafts[ADMIN_ID] = {"mode": action}
        await query.message.reply_text(f"এখন যে user ID {action} করবেন সেটি শুধু সংখ্যা হিসেবে পাঠান।")


async def create_broadcast(update, context, target, content_type, text_content, file_id):
    bid = await q(
        "INSERT INTO controller_broadcasts(admin_id,target,content_type,text_content,file_id,status) VALUES(%s,%s,%s,%s,%s,'queued')",
        (ADMIN_ID, target, content_type, text_content, file_id)
    )
    targets = []
    if target in ("users", "both"):
        rows = await q("SELECT user_id FROM controller_users", fetch=True, many=True)
        targets += [(r["user_id"], "user") for r in rows or []]
    if target in ("channels", "both"):
        rows = await q("SELECT DISTINCT chat_id FROM controller_channels", fetch=True, many=True)
        targets += [(r["chat_id"], "channel") for r in rows or []]
    for tid, typ in targets:
        await q("INSERT IGNORE INTO controller_broadcast_targets(broadcast_id,target_id,target_type) VALUES(%s,%s,%s)",
                (bid, tid, typ))
    await q("UPDATE controller_broadcasts SET status='running' WHERE id=%s", (bid,))
    sent = failed = 0
    for tid, typ in targets:
        try:
            if content_type == "photo":
                await context.bot.send_photo(tid, file_id, caption=text_content or None)
            else:
                await context.bot.send_message(tid, text_content or "")
            await q("UPDATE controller_broadcast_targets SET status='sent' WHERE broadcast_id=%s AND target_id=%s AND target_type=%s",
                    (bid, tid, typ))
            sent += 1
        except RetryAfter as e:
            await asyncio.sleep(float(e.retry_after) + 1)
            try:
                if content_type == "photo": await context.bot.send_photo(tid, file_id, caption=text_content or None)
                else: await context.bot.send_message(tid, text_content or "")
                sent += 1
            except Exception as ex:
                failed += 1
                await q("UPDATE controller_broadcast_targets SET status='failed',error_text=%s WHERE broadcast_id=%s AND target_id=%s AND target_type=%s",
                        (str(ex)[:500], bid, tid, typ))
        except Exception as e:
            failed += 1
            await q("UPDATE controller_broadcast_targets SET status='failed',error_text=%s WHERE broadcast_id=%s AND target_id=%s AND target_type=%s",
                    (str(e)[:500], bid, tid, typ))
        await asyncio.sleep(0.05)
    await q("UPDATE controller_broadcasts SET status='completed',sent_count=%s,failed_count=%s,finished_at=UTC_TIMESTAMP() WHERE id=%s",
            (sent, failed, bid))
    await update.effective_message.reply_text(f"📢 Broadcast finished.\nSent: {sent}\nFailed: {failed}")


async def text_router(update, context):
    msg = update.effective_message
    user = update.effective_user
    if not msg or not user:
        return
    text = (msg.text or "").strip()
    if user.id == ADMIN_ID and user.id in drafts and drafts[user.id].get("mode") in ("ban", "unban"):
        mode = drafts.pop(user.id)["mode"]
        try:
            target = int(text)
            if mode == "ban":
                await q("INSERT IGNORE INTO banned_users(user_id,reason,banned_by) VALUES(%s,%s,%s)",
                        (target, "Admin action", ADMIN_ID))
                await msg.reply_text(f"⛔ User {target} banned.")
            else:
                await q("DELETE FROM banned_users WHERE user_id=%s", (target,))
                await msg.reply_text(f"✅ User {target} unbanned.")
        except ValueError:
            await msg.reply_text("সঠিক numeric Telegram user ID দিন।")
        return
    if text == "📝 Create Post": return await create_post(update, context)
    if text == "📣 Broadcast":
        if user.id == ADMIN_ID: return await admin_panel(update, context)
        return await msg.reply_text("এই অপশনটি শুধু অ্যাডমিনের জন্য।")
    if text == "📡 My Channels": return await my_channels(update, context)
    if text == "⏰ Scheduled Posts": return await scheduled_list(update, context)
    if text == "🆘 Support": return await support(update, context)
    if text == "👑 Admin Panel":
        if user.id == ADMIN_ID: return await admin_panel(update, context)
        return await msg.reply_text("⛔ Admin only.")
    await capture_content(update, context)


async def error_handler(update, context):
    log.error("Unhandled update error", exc_info=context.error)


async def post_init(app):
    await init_db()
    app.job_queue.run_repeating(scheduler_job, interval=15, first=5, name="scheduled-posts")
    log.info("Bot started; scheduler enabled.")


async def health(_request):
    # Render HTTP health checks expect a 2xx/3xx response.
    return web.json_response({"status": "ok", "service": "telegram-controller-bot"})


async def index(_request):
    return web.json_response({
        "name": "Telegram Controller Bot",
        "status": "running",
        "health": "/health"
    })


async def start_http_server():
    port = int(os.getenv("PORT", "10000"))
    web_app = web.Application()
    web_app.router.add_get("/", index)
    web_app.router.add_get("/health", health)
    runner = web.AppRunner(web_app, access_log=log)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    log.info("HTTP health server listening on 0.0.0.0:%s", port)
    return runner


async def run_bot():
    bot_app = Application.builder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("addchannel", addchannel))
    bot_app.add_handler(CommandHandler("addbutton", add_button))
    bot_app.add_handler(CommandHandler("publish", publish_draft))
    bot_app.add_handler(CommandHandler("schedule", schedule_draft))
    bot_app.add_handler(CallbackQueryHandler(admin_callbacks, pattern=r"^admin:"))
    bot_app.add_handler(CallbackQueryHandler(callback))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    bot_app.add_handler(MessageHandler((filters.PHOTO | filters.VIDEO) & ~filters.COMMAND, capture_content))
    bot_app.add_error_handler(error_handler)

    await bot_app.initialize()
    await post_init(bot_app)
    await bot_app.start()
    if bot_app.updater is None:
        raise RuntimeError("Telegram updater is unavailable; polling cannot start")
    await bot_app.updater.start_polling(
        drop_pending_updates=False,
        allowed_updates=Update.ALL_TYPES
    )
    log.info("Telegram polling started.")
    try:
        await asyncio.Event().wait()
    finally:
        if bot_app.updater.running:
            await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()
        global pool
        if pool is not None:
            pool.close()
            await pool.wait_closed()


async def main_async():
    # Start HTTP first so Render can detect the assigned PORT quickly.
    runner = await start_http_server()
    try:
        await run_bot()
    finally:
        await runner.cleanup()


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("Shutdown requested.")


if __name__ == "__main__":
    main()
