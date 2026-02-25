"""
scheduler_bot.py  ─  Auto-Scheduler Bot (multi-tenant, PostgreSQL)
───────────────────────────────────────────────────────────────────
All 4 scheduling modes (bulk, batch, exact, duration) work exactly
as before.  The only change is the storage backend: PostgreSQL via
db.py instead of a local SQLite file.

ENV VARS
────────
  BOT_TOKEN              — Telegram bot token  (unique per instance)
  ADMIN_ID               — Your Telegram user ID
  DATABASE_URL           — Shared PostgreSQL URL  (same for all bots)
  CHANNEL_IDS            — Optional comma-separated startup channels
  AUTO_CLEANUP_MINUTES   — Minutes to keep posted records  (default 30)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta

import db  # shared DB layer
import pytz
from telegram import (KeyboardButton, ReplyKeyboardMarkup,
                      ReplyKeyboardRemove, Update)
from telegram.error import NetworkError, TelegramError, TimedOut
from telegram.ext import (Application, CommandHandler, ContextTypes,
                          MessageHandler, filters)

# ─────────────────────────────────────────────
# Windows encoding fix
# ─────────────────────────────────────────────
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# ─────────────────────────────────────────────
# Timezone
# ─────────────────────────────────────────────
IST = pytz.timezone('Asia/Kolkata')

logging.basicConfig(
    format='%(asctime)s [%(name)s] %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('scheduler_bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# UTC / IST helpers
# ─────────────────────────────────────────────
def utc_now():
    return datetime.utcnow()

def ist_to_utc(ist_dt):
    aware = IST.localize(ist_dt) if ist_dt.tzinfo is None else ist_dt
    return aware.astimezone(pytz.UTC).replace(tzinfo=None)

def utc_to_ist(utc_dt):
    aware = pytz.UTC.localize(utc_dt) if utc_dt.tzinfo is None else utc_dt
    return aware.astimezone(IST).replace(tzinfo=None)

def get_ist_now():
    return utc_to_ist(utc_now())


# ─────────────────────────────────────────────
# Module-level state
# ─────────────────────────────────────────────
BOT_ID: str = ''
ADMIN_ID: int = 0
AUTO_CLEANUP_MINUTES: int = 30
channel_ids: list[str] = []          # in-memory cache
user_sessions: dict = {}
posting_lock: asyncio.Lock = None    # created in main()


def reload_channels():
    global channel_ids
    channel_ids = db.channel_list_active(BOT_ID)
    logger.info(f"📢 Channels loaded: {len(channel_ids)}")


def _is_admin(update: Update) -> bool:
    return update.effective_user and update.effective_user.id == ADMIN_ID


# ─────────────────────────────────────────────
# Keyboards
# ─────────────────────────────────────────────
def get_mode_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📦 Bulk Posts (Auto-Space)")],
        [KeyboardButton("🎯 Bulk Posts (Batches)")],
        [KeyboardButton("📆 Multi-Day Batch")],
        [KeyboardButton("📋 View Pending"), KeyboardButton("📊 Stats")],
        [KeyboardButton("📢 Channels"),     KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)

def get_bulk_collection_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("✅ Done - Schedule All Posts")],
        [KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)

def get_confirmation_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("✅ Confirm & Schedule")],
        [KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)

def get_duration_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("2h"), KeyboardButton("6h"), KeyboardButton("12h")],
        [KeyboardButton("1d"), KeyboardButton("today")],
        [KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)

def get_quick_time_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("5m"), KeyboardButton("30m"), KeyboardButton("1h")],
        [KeyboardButton("2h"), KeyboardButton("now")],
        [KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)

def get_exact_time_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("today 18:00"), KeyboardButton("tomorrow 9am")],
        [KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)

def get_batch_size_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("10"), KeyboardButton("20"), KeyboardButton("30")],
        [KeyboardButton("50"), KeyboardButton("100")],
        [KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)

def get_batch_type_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📊 Normal Batch")],
        [KeyboardButton("⚡ Auto-Continuous")],
        [KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)

def get_days_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("1"), KeyboardButton("2"), KeyboardButton("3")],
        [KeyboardButton("5"), KeyboardButton("7"), KeyboardButton("14")],
        [KeyboardButton("30"), KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)

def get_multiday_type_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📊 Normal Multi-Day")],
        [KeyboardButton("⚡ Ex-AutoContinuous")],
        [KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)

def get_time_of_day_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("20:00"), KeyboardButton("21:00"), KeyboardButton("22:00")],
        [KeyboardButton("18:00"), KeyboardButton("19:00"), KeyboardButton("23:00")],
        [KeyboardButton("❌ Cancel")],
    ], resize_keyboard=True)


# ─────────────────────────────────────────────
# Time parsing
# ─────────────────────────────────────────────
import re

def parse_duration_to_minutes(text: str) -> int:
    text = text.strip().lower()
    if text == 'today':
        now = get_ist_now()
        midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
        return int((midnight - now).total_seconds() / 60)
    if text.endswith('m'):  return int(text[:-1])
    if text.endswith('h'):  return int(text[:-1]) * 60
    if text.endswith('d'):  return int(text[:-1]) * 1440
    raise ValueError("Use: 30m, 2h, 1d, today")

def parse_hour(text: str) -> int:
    text = text.strip().lower()
    if text.endswith('am'):
        h = int(re.sub(r'[^0-9]', '', text))
        return 0 if h == 12 else h
    if text.endswith('pm'):
        h = int(re.sub(r'[^0-9]', '', text))
        return h if h == 12 else h + 12
    if ':' in text:
        return int(text.split(':')[0])
    return int(text)

def parse_user_time_input(text: str) -> datetime:
    text = text.strip().lower()
    now = get_ist_now()
    if text == 'now':      return now
    if text.endswith('m'): return now + timedelta(minutes=int(text[:-1]))
    if text.endswith('h'): return now + timedelta(hours=int(text[:-1]))
    if text.endswith('d'): return now + timedelta(days=int(text[:-1]))
    if text.startswith('tomorrow'):
        tp = text.replace('tomorrow', '').strip()
        base = (now + timedelta(days=1)).date()
        h = parse_hour(tp) if tp else 9
        return datetime.combine(base, datetime.min.time()) + timedelta(hours=h)
    if text.startswith('today'):
        tp = text.replace('today', '').strip()
        h = parse_hour(tp) if tp else 0
        return datetime.combine(now.date(), datetime.min.time()) + timedelta(hours=h)
    for fmt in ('%Y-%m-%d %H:%M', '%m/%d %H:%M', '%d/%m %H:%M'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError(
        f"Cannot parse '{text}'.\n"
        "Try: now, 30m, 2h, today 18:00, tomorrow 9am, 2025-12-25 09:00"
    )


# ─────────────────────────────────────────────
# Content extraction
# ─────────────────────────────────────────────
_BTN = ["✅ Done", "❌ Cancel", "✅ Confirm", "📦 Bulk", "📅 Exact",
        "⏱️ Duration", "📋 View", "📊 Stats", "📢 Channels",
        "Schedule All", "Confirm & Schedule", "🎯 Bulk"]

def extract_content(message) -> dict | None:
    c = {}
    if message.text and not message.text.startswith('/'):
        if not any(k in message.text for k in _BTN):
            c['message'] = message.text
    if message.photo:
        c.update(media_type='photo',    media_file_id=message.photo[-1].file_id, caption=message.caption)
    elif message.video:
        c.update(media_type='video',    media_file_id=message.video.file_id,     caption=message.caption)
    elif message.document:
        c.update(media_type='document', media_file_id=message.document.file_id,  caption=message.caption)
    return c if c else None


# ─────────────────────────────────────────────
# Posting engine
# ─────────────────────────────────────────────
async def send_to_all_channels(bot, post: dict) -> int:
    successful = 0

    async def _send(ch_id: str, max_retries: int = 5):
        for attempt in range(max_retries):
            try:
                kw = dict(read_timeout=60, write_timeout=60, connect_timeout=60)
                if post['media_type'] == 'photo':
                    await bot.send_photo(ch_id, post['media_file_id'],
                                         caption=post['caption'], **kw)
                elif post['media_type'] == 'video':
                    await bot.send_video(ch_id, post['media_file_id'],
                                         caption=post['caption'], **kw)
                elif post['media_type'] == 'document':
                    await bot.send_document(ch_id, post['media_file_id'],
                                            caption=post['caption'], **kw)
                else:
                    await bot.send_message(ch_id, post['message'], **kw)
                return {'success': True, 'channel_id': ch_id}
            except (TimedOut, NetworkError) as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep((attempt + 1) * 3)
                else:
                    logger.error(f"❌ Gave up on {ch_id}: {e}")
                    add_to_skip_list(ch_id, minutes=5)
                    return {'success': False, 'channel_id': ch_id}
            except TelegramError as e:
                logger.error(f"❌ TelegramError {ch_id}: {e}")
                add_to_skip_list(ch_id, minutes=5)
                return {'success': False, 'channel_id': ch_id}

    batch_size = 20
    failed_channels = []
    
    for i in range(0, len(channel_ids), batch_size):
        results = await asyncio.gather(*[_send(ch) for ch in channel_ids[i:i+batch_size]])
        for r in results:
            if isinstance(r, dict):
                if r['success']:
                    successful += 1
                else:
                    failed_channels.append(r['channel_id'])
        if i + batch_size < len(channel_ids):
            await asyncio.sleep(2.0)

    db.post_mark_sent(BOT_ID, post['id'], successful)
    logger.info(f"📊 Post {post['id']}: {successful}/{len(channel_ids)} channels")
    
    # Notify admin if there were failures
    if failed_channels and ADMIN_ID:
        try:
            failed_list = '\n'.join([f"• <code>{ch}</code>" for ch in failed_channels[:10]])
            if len(failed_channels) > 10:
                failed_list += f"\n<i>...and {len(failed_channels)-10} more</i>"
            
            from telegram import Bot
            bot_instance = Bot(token=os.getenv('BOT_TOKEN'))
            import asyncio
            asyncio.create_task(bot_instance.send_message(
                ADMIN_ID,
                f"⚠️ <b>POST #{post['id']} FAILED</b>\n\n"
                f"✅ Successful: {successful}/{len(channel_ids)}\n"
                f"❌ Failed: {len(failed_channels)}\n\n"
                f"<b>Failed channels (skipped 5min):</b>\n"
                f"{failed_list}\n\n"
                f"💡 Will retry on next post",
                parse_mode='HTML'
            ))
        except Exception as e:
            logger.error(f"Failed to notify admin: {e}")
    
    return successful


async def process_due_posts(bot):
    async with posting_lock:
        posts = db.post_get_due(BOT_ID, limit=200)
        for post in posts:
            await send_to_all_channels(bot, post)
            await asyncio.sleep(1)


def cleanup_posted_content() -> int:
    n = db.post_cleanup_old(BOT_ID, AUTO_CLEANUP_MINUTES)
    if n:
        logger.info(f"🧹 Cleaned {n} old posts")
    return n


# ─────────────────────────────────────────────
# Command handlers
# ─────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    s = db.post_stats(BOT_ID)
    await update.message.reply_text(
        f"👋 <b>Telegram Scheduler Bot</b>\n\n"
        f"🤖 Bot ID: <code>{BOT_ID}</code>\n"
        f"📢 Active Channels: <b>{len(channel_ids)}</b>\n"
        f"📋 Pending Posts: <b>{s['pending']}</b>\n"
        f"🧹 Auto-cleanup: <b>{AUTO_CLEANUP_MINUTES} min</b>\n\n"
        f"Choose a mode:",
        reply_markup=get_mode_keyboard(), parse_mode='HTML'
    )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    s = db.post_stats(BOT_ID)
    await update.message.reply_text(
        f"📊 <b>STATISTICS</b>\n\n"
        f"🤖 Bot ID: <code>{BOT_ID}</code>\n"
        f"📢 Channels: <b>{len(channel_ids)}</b>\n"
        f"📋 Pending: <b>{s['pending']}</b>\n"
        f"✅ Posted:  <b>{s['posted']}</b>\n"
        f"📦 Total:   <b>{s['total']}</b>\n"
        f"🧹 Cleanup: {AUTO_CLEANUP_MINUTES} min",
        reply_markup=get_mode_keyboard(), parse_mode='HTML'
    )

async def channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    chs = db.channel_list_all(BOT_ID)
    if not chs:
        await update.message.reply_text(
            "📢 <b>No channels!</b>\n/addchannel -100xxx",
            reply_markup=get_mode_keyboard(), parse_mode='HTML'); return
    active = sum(1 for c in chs if c['active'])
    resp = f"📢 <b>CHANNELS ({len(chs)})</b>\n\n"
    for ch in chs:
        icon = "✅" if ch['active'] else "❌"
        resp += f"{icon} <code>{ch['channel_id']}</code>\n   {ch['channel_name'] or 'Unnamed'}\n\n"
    resp += f"Active: {active} | Inactive: {len(chs)-active}"
    await update.message.reply_text(resp, reply_markup=get_mode_keyboard(), parse_mode='HTML')

async def add_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    if not context.args:
        await update.message.reply_text(
            "❌ Usage: <code>/addchannel -100xxx Name</code>",
            reply_markup=get_mode_keyboard(), parse_mode='HTML'); return
    cid  = context.args[0]
    name = " ".join(context.args[1:]) or None
    db.channel_add(BOT_ID, cid, name)
    reload_channels()
    await update.message.reply_text(
        f"✅ Added <code>{cid}</code>  Active: <b>{len(channel_ids)}</b>",
        reply_markup=get_mode_keyboard(), parse_mode='HTML')

async def remove_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    if not context.args:
        await update.message.reply_text(
            "❌ Usage: <code>/removechannel -100xxx</code>",
            reply_markup=get_mode_keyboard(), parse_mode='HTML'); return
    cid = context.args[0]
    if db.channel_remove(BOT_ID, cid):
        reload_channels()
        await update.message.reply_text(
            f"✅ Removed <code>{cid}</code>  Remaining: <b>{len(channel_ids)}</b>",
            reply_markup=get_mode_keyboard(), parse_mode='HTML')
    else:
        await update.message.reply_text(
            f"❌ Not found: <code>{cid}</code>",
            reply_markup=get_mode_keyboard(), parse_mode='HTML')

async def list_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    posts = db.post_get_pending(BOT_ID)
    if not posts:
        await update.message.reply_text("✅ No pending posts!", reply_markup=get_mode_keyboard()); return
    resp = f"📋 <b>Pending ({len(posts)})</b>\n\n"
    for p in posts[:10]:
        t = p['scheduled_time']
        if hasattr(t, 'tzinfo') and t.tzinfo:
            t = t.replace(tzinfo=None)
        ist = utc_to_ist(t)
        content = p['message'] or p['caption'] or f"[{p['media_type']}]"
        preview = content[:25] + "…" if len(content) > 25 else content
        resp += f"🆔 {p['id']} — {ist.strftime('%m/%d %H:%M')} IST\n   {preview}\n\n"
    if len(posts) > 10:
        resp += f"<i>…and {len(posts)-10} more</i>\n"
    resp += "\n/delete [id]"
    await update.message.reply_text(resp, parse_mode='HTML', reply_markup=get_mode_keyboard())

async def delete_post_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    if not context.args:
        await update.message.reply_text("Usage: /delete [id]"); return
    try:
        pid = int(context.args[0])
        if db.post_delete(BOT_ID, pid):
            await update.message.reply_text(f"✅ Deleted #{pid}", reply_markup=get_mode_keyboard())
        else:
            await update.message.reply_text(f"❌ #{pid} not found", reply_markup=get_mode_keyboard())
    except ValueError:
        await update.message.reply_text("Invalid ID")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    user_sessions[update.effective_user.id] = {'mode': None, 'step': 'choose_mode'}
    await update.message.reply_text("❌ Cancelled.", reply_markup=get_mode_keyboard())

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    if not context.args or context.args[0].lower() != 'confirm':
        await update.message.reply_text(
            "⚠️ Deletes ALL pending posts.\nConfirm: <code>/reset confirm</code>",
            reply_markup=get_mode_keyboard(), parse_mode='HTML'); return
    n = db.post_delete_pending_all(BOT_ID)
    await update.message.reply_text(f"✅ Reset — deleted {n} posts.", reply_markup=get_mode_keyboard())

async def export_channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    chs = db.channel_list_all(BOT_ID)
    if not chs:
        await update.message.reply_text("No channels.", reply_markup=get_mode_keyboard()); return
    
    active_chs = [c for c in chs if c['active']]
    
    # Send header
    await update.message.reply_text(
        f"📤 <b>EXPORTING {len(active_chs)} CHANNELS</b>\n\n"
        f"⬇️ Forward each message back to bot to restore\n"
        f"💡 Select all → Forward",
        parse_mode='HTML')
    
    # Send each command separately
    for c in active_chs:
        cmd = f"/addchannel {c['channel_id']}"
        if c['channel_name']:
            cmd += f" {c['channel_name']}"
        await update.message.reply_text(cmd)
        await asyncio.sleep(0.2)  # Avoid flood
    
    # Send footer
    await update.message.reply_text(
        f"✅ <b>Exported {len(active_chs)} channels!</b>\n\n"
        f"Select all commands above and forward to bot to restore",
        parse_mode='HTML',
        reply_markup=get_mode_keyboard()
    )

async def backup_posts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    posts = db.post_get_pending(BOT_ID)
    if not posts:
        await update.message.reply_text("No pending posts.", reply_markup=get_mode_keyboard()); return
    text = f"📦 <b>POSTS BACKUP</b>\n{len(posts)} pending\n\n"
    for p in posts[:50]:
        t = p['scheduled_time']
        if hasattr(t, 'tzinfo') and t.tzinfo:
            t = t.replace(tzinfo=None)
        ist = utc_to_ist(t)
        text += f"🆔 #{p['id']}  {ist.strftime('%Y-%m-%d %H:%M')} IST\n"
        if p['message']: text += f"   {p['message'][:40]}\n"
        elif p['media_type']: text += f"   [{p['media_type']}]\n"
        text += "\n"
    await update.message.reply_text(text, parse_mode='HTML', reply_markup=get_mode_keyboard())


# ─────────────────────────────────────────────
# Scheduling helpers
# ─────────────────────────────────────────────
async def _schedule_bulk(update, session):
    posts = session['posts']
    dur   = session['duration_minutes']
    start = session['bulk_start_time_utc']
    n     = len(posts)
    intv  = dur / n if n > 1 else 0
    info  = []
    for i, p in enumerate(posts):
        t = start + timedelta(minutes=intv * i)
        pid = db.post_insert(BOT_ID, t, len(channel_ids),
                             p.get('message'), p.get('media_type'),
                             p.get('media_file_id'), p.get('caption'))
        info.append((pid, t))
    s_ist = utc_to_ist(start)
    resp  = (f"✅ <b>BULK SCHEDULED!</b>\n\n"
             f"📦 {n} posts  📢 {len(channel_ids)} channels\n"
             f"🕐 {s_ist.strftime('%Y-%m-%d %H:%M')} IST  ⏱️ {dur} min  interval {intv:.1f} min\n\n")
    for pid, t in info[:5]:
        resp += f"• {utc_to_ist(t).strftime('%H:%M')} IST — #{pid}\n"
    if n > 5: resp += f"<i>…and {n-5} more</i>\n"
    await update.message.reply_text(resp, reply_markup=get_mode_keyboard(), parse_mode='HTML')

async def _schedule_batch(update, session):
    posts = session['posts']
    dur   = session['duration_minutes']
    bs    = session['batch_size']
    start = session['batch_start_time_utc']
    n     = len(posts)
    nb    = (n + bs - 1) // bs
    bi    = dur / nb if nb > 1 else 0
    info  = []
    for i, p in enumerate(posts):
        bn = i // bs
        t  = start + timedelta(minutes=bi * bn, seconds=(i % bs) * 2)
        pid = db.post_insert(BOT_ID, t, len(channel_ids),
                             p.get('message'), p.get('media_type'),
                             p.get('media_file_id'), p.get('caption'))
        info.append((pid, t, bn + 1))
    s_ist = utc_to_ist(start)
    resp  = (f"✅ <b>BATCH SCHEDULED!</b>\n\n"
             f"📦 {n} posts  🎯 {bs}/batch  📊 {nb} batches\n"
             f"📢 {len(channel_ids)} channels  🕐 {s_ist.strftime('%Y-%m-%d %H:%M')} IST\n\n")
    cur_b = 0
    for pid, t, bn in info[:10]:
        if bn != cur_b:
            if cur_b: resp += "\n"
            resp += f"<b>Batch #{bn}</b> {utc_to_ist(t).strftime('%H:%M')} IST:\n"
            cur_b = bn
        resp += f"  • #{pid}\n"
    if n > 10: resp += f"<i>…and {n-10} more</i>"
    await update.message.reply_text(resp, reply_markup=get_mode_keyboard(), parse_mode='HTML')



async def _schedule_autocont(update, session):
    """Auto-Continuous scheduler - fixed interval between batches."""
    posts = session['posts']
    batch_size = session['batch_size']
    interval_minutes = session['interval_minutes']
    start_utc = session['auto_start_time_utc']
    
    n = len(posts)
    num_batches = (n + batch_size - 1) // batch_size
    info = []
    
    for i, p in enumerate(posts):
        batch_number = i // batch_size
        post_in_batch = i % batch_size
        batch_time = start_utc + timedelta(minutes=interval_minutes * batch_number)
        t = batch_time + timedelta(seconds=post_in_batch * 2)
        pid = db.post_insert(BOT_ID, t, len(channel_ids),
                             p.get('message'), p.get('media_type'),
                             p.get('media_file_id'), p.get('caption'))
        info.append((pid, t, batch_number + 1))
    
    h, m = divmod(interval_minutes, 60)
    interval_display = f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")
    
    resp = (f"✅ <b>AUTO-CONTINUOUS SCHEDULED!</b>\n\n"
            f"📦 {n} posts  🎯 {batch_size}/batch  📊 {num_batches} batches\n"
            f"⏱️ Interval: every {interval_display}\n"
            f"📢 {len(channel_ids)} channels\n\n"
            f"<b>Batch preview:</b>\n")
    
    cur_b = 0
    shown = 0
    for pid, t, bn in info:
        if shown >= 10:
            resp += f"<i>...and {len(info)-shown} more</i>"
            break
        if bn != cur_b:
            if cur_b:
                resp += "\n"
            resp += f"<b>Batch #{bn}</b> {utc_to_ist(t).strftime('%H:%M')} IST:\n"
            cur_b = bn
        resp += f"  • #{pid}\n"
        shown += 1
    
    resp += f"\n💡 Add more posts anytime — they'll continue from Batch #{num_batches + 1}"
    await update.message.reply_text(resp, reply_markup=get_mode_keyboard(), parse_mode='HTML')


async def _schedule_exautocont(update, session):
    """Ex-AutoContinuous Multi-Day scheduler with daily window + intervals."""
    posts = session['posts']
    batch_size = session['batch_size']
    interval_min = session['exautocont_interval_minutes']
    first_batch_utc = session['exautocont_first_batch_utc']
    daily_start_h = session['exautocont_daily_start_hour']
    
    # Calculate window
    if session['exautocont_window_is_duration']:
        window_minutes = session['exautocont_window_duration_minutes']
    else:
        end_h = session['exautocont_daily_end_hour']
        if end_h > daily_start_h:
            window_minutes = (end_h - daily_start_h) * 60
        else:
            window_minutes = (24 - daily_start_h + end_h) * 60
    
    n = len(posts)
    first_batch_ist = utc_to_ist(first_batch_utc)
    first_batch_hour = first_batch_ist.hour
    first_batch_date = first_batch_ist.date()
    
    # Calculate Day 1 capacity (from first batch time to end of window)
    if session['exautocont_window_is_duration']:
        # Duration-based: first batch + duration
        day1_minutes = window_minutes - ((first_batch_hour - daily_start_h) * 60 if first_batch_hour >= daily_start_h else 0)
    else:
        # Time-based: first batch to end time
        end_h = session['exautocont_daily_end_hour']
        if first_batch_hour <= end_h:
            day1_minutes = (end_h - first_batch_hour) * 60
        else:
            day1_minutes = (24 - first_batch_hour + end_h) * 60
    
    day1_batches = int(day1_minutes / interval_min) + 1  # +1 for the first batch itself
    day1_capacity = day1_batches * batch_size
    
    info = []
    post_idx = 0
    current_time = first_batch_utc
    current_day = 0
    
    # Schedule all posts
    while post_idx < n:
        batch_posts = posts[post_idx:post_idx + batch_size]
        for i, p in enumerate(batch_posts):
            t = current_time + timedelta(seconds=i * 2)
            pid = db.post_insert(BOT_ID, t, len(channel_ids),
                                 p.get('message'), p.get('media_type'),
                                 p.get('media_file_id'), p.get('caption'))
            info.append((pid, t, current_day + 1))
        
        post_idx += len(batch_posts)
        current_time += timedelta(minutes=interval_min)
        
        # Check if we need to move to next day
        current_ist = utc_to_ist(current_time)
        if session['exautocont_window_is_duration']:
            # Duration mode: check if we exceeded window duration from daily start
            daily_start_today = datetime.combine(current_ist.date(), datetime.min.time()) + timedelta(hours=daily_start_h)
            minutes_since_start = (current_ist - daily_start_today).total_seconds() / 60
            if minutes_since_start >= window_minutes:
                # Move to next day
                current_day += 1
                next_day = current_ist.date() + timedelta(days=1)
                current_time = ist_to_utc(datetime.combine(next_day, datetime.min.time()) + timedelta(hours=daily_start_h))
        else:
            # Time mode: check if we passed end hour
            end_h = session['exautocont_daily_end_hour']
            if (end_h > daily_start_h and current_ist.hour >= end_h) or \
               (end_h <= daily_start_h and current_ist.hour >= end_h and current_ist.hour < daily_start_h):
                # Move to next day
                current_day += 1
                next_day = current_ist.date() + timedelta(days=1)
                current_time = ist_to_utc(datetime.combine(next_day, datetime.min.time()) + timedelta(hours=daily_start_h))
    
    # Build response
    days_used = len(set(x[2] for x in info))
    h, m = divmod(interval_min, 60)
    interval_display = f"{h}h {m}m" if m else f"{h}h"
    
    resp = (f"✅ <b>EX-AUTOCONTINUOUS SCHEDULED!</b>\n\n"
            f"📦 {n} posts  📆 {days_used} days  🎯 {batch_size}/batch\n"
            f"⏱️ Interval: {interval_display}\n"
            f"📢 {len(channel_ids)} channels\n\n")
    
    cur_day = 0
    shown = 0
    for pid, t, day_no in info:
        if shown >= 12:
            resp += f"<i>…and {len(info)-shown} more</i>"
            break
        if day_no != cur_day:
            ist_day = utc_to_ist(t)
            resp += f"\n<b>Day {day_no} — {ist_day.strftime('%b %d')}:</b>\n"
            cur_day = day_no
        resp += f"  • {utc_to_ist(t).strftime('%H:%M')} IST — #{pid}\n"
        shown += 1
    
    await update.message.reply_text(resp, reply_markup=get_mode_keyboard(), parse_mode='HTML')


async def _schedule_multiday(update, session):
    """Schedule posts across multiple days within a daily time window."""
    posts       = session['posts']
    days        = session['multiday_days']
    batch_size  = session['batch_size']
    start_h     = session['window_start_hour']
    end_h       = session['window_end_hour']
    first_date  = session['first_date_utc']
    
    n = len(posts)
    if end_h > start_h:
        window_minutes = (end_h - start_h) * 60
    else:
        window_minutes = (24 - start_h + end_h) * 60
    
    info = []
    for day in range(days):
        start_idx = round(day * n / days)
        end_idx = round((day + 1) * n / days)
        day_posts = posts[start_idx:end_idx]
        if not day_posts:
            continue
        
        window_start_utc = first_date + timedelta(days=day, hours=start_h)
        nd = len(day_posts)
        nb = (nd + batch_size - 1) // batch_size
        bi = window_minutes / nb if nb > 1 else 0
        
        for i, p in enumerate(day_posts):
            bn = i // batch_size
            t = window_start_utc + timedelta(minutes=bi * bn, seconds=(i % batch_size) * 2)
            pid = db.post_insert(BOT_ID, t, len(channel_ids),
                                 p.get('message'), p.get('media_type'),
                                 p.get('media_file_id'), p.get('caption'))
            info.append((pid, t, day + 1))
    
    resp = (f"✅ <b>MULTI-DAY BATCH SCHEDULED!</b>\n\n"
            f"📦 {n} posts  📆 {days} days  🎯 {batch_size}/batch\n"
            f"🕐 Window: {start_h:02d}:00 – {end_h:02d}:00 IST  ({window_minutes} min)\n"
            f"📢 {len(channel_ids)} channels\n\n")
    
    cur_day = 0
    shown = 0
    for pid, t, day_no in info:
        if shown >= 12:
            resp += f"<i>…and {len(info)-shown} more</i>"
            break
        if day_no != cur_day:
            ist_day = utc_to_ist(t)
            resp += f"\n<b>Day {day_no} — {ist_day.strftime('%b %d')}:</b>\n"
            cur_day = day_no
        resp += f"  • {utc_to_ist(t).strftime('%H:%M')} IST — #{pid}\n"
        shown += 1
    
    await update.message.reply_text(resp, reply_markup=get_mode_keyboard(), parse_mode='HTML')


# ─────────────────────────────────────────────
# Main message handler (conversation FSM)
# ─────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not _is_admin(update): return

    uid  = update.effective_user.id
    user_sessions.setdefault(uid, {'mode': None, 'step': 'choose_mode'})
    sess = user_sessions[uid]
    text = (update.message.text or "").strip()

    # Quick shortcuts
    if "📊 Stats"  in text: await stats_command(update, context); return
    if "📢 Channels" in text: await channels_command(update, context); return
    if "📋 View"   in text: await list_posts(update, context); return
    if "❌" in text or text.lower() == "cancel": await cancel(update, context); return

    def _no_ch():
        return len(channel_ids) == 0

    # ════ CHOOSE MODE ════════════════════════
    if sess['step'] == 'choose_mode':

        if "📦 Bulk" in text:
            if _no_ch():
                await update.message.reply_text("❌ Add a channel first: /addchannel -100xxx",
                                                 reply_markup=get_mode_keyboard()); return
            sess.update(mode='bulk', step='bulk_get_start_time', posts=[])
            await update.message.reply_text(
                f"📦 <b>BULK MODE</b>\n🕐 Now: <b>{get_ist_now().strftime('%H:%M:%S')} IST</b>\n\n"
                "When should the first post go out?\n<code>now  30m  2h  today 18:00  tomorrow 9am</code>",
                reply_markup=get_exact_time_keyboard(), parse_mode='HTML')

        elif "🎯 Bulk" in text:
            if _no_ch():
                await update.message.reply_text("❌ Add a channel first.", reply_markup=get_mode_keyboard()); return
            sess.update(mode='batch', step='batch_choose_type', posts=[])
            await update.message.reply_text(
                f"🎯 <b>BATCH MODE</b>\n\n"
                "Choose batch scheduling type:",
                reply_markup=get_batch_type_keyboard(), parse_mode='HTML')

        elif "📆 Multi-Day" in text:
            if _no_ch():
                await update.message.reply_text("❌ Add a channel first.", reply_markup=get_mode_keyboard()); return
            sess.update(mode='multiday', step='multiday_choose_type', posts=[])
            await update.message.reply_text(
                f"📆 <b>MULTI-DAY BATCH MODE</b>\n\n"
                "Choose scheduling type:",
                reply_markup=get_multiday_type_keyboard(), parse_mode='HTML')

        elif "📅 Exact" in text:
            if _no_ch():
                await update.message.reply_text("❌ Add a channel first.", reply_markup=get_mode_keyboard()); return
            sess.update(mode='exact', step='exact_get_time')
            await update.message.reply_text(
                f"📅 <b>EXACT TIME MODE</b>\n🕐 Now: <b>{get_ist_now().strftime('%H:%M:%S')} IST</b>\n\n"
                "When to post?\n<code>2025-12-31 23:59  tomorrow 2pm  today 18:00</code>",
                reply_markup=get_exact_time_keyboard(), parse_mode='HTML')

        elif "⏱️ Duration" in text:
            if _no_ch():
                await update.message.reply_text("❌ Add a channel first.", reply_markup=get_mode_keyboard()); return
            sess.update(mode='duration', step='duration_get_time')
            await update.message.reply_text(
                "⏱️ <b>DURATION MODE</b>\n\nHow long to wait?\n<code>15m  3h  2d</code>",
                reply_markup=get_quick_time_keyboard(), parse_mode='HTML')

        else:
            await update.message.reply_text("Choose a mode:", reply_markup=get_mode_keyboard())
        return

    # ════ BULK ═══════════════════════════════
    elif sess['mode'] == 'bulk':

        if sess['step'] == 'bulk_get_start_time':
            try:
                ist = parse_user_time_input(text)
                sess['bulk_start_time_utc'] = ist_to_utc(ist)
                sess['step'] = 'bulk_get_duration'
                await update.message.reply_text(
                    f"✅ Start: <b>{ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n\n"
                    "Total duration to spread posts?\n<code>2h  6h  12h  1d</code>",
                    reply_markup=get_duration_keyboard(), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=get_exact_time_keyboard())
            return

        if sess['step'] == 'bulk_get_duration':
            try:
                sess['duration_minutes'] = parse_duration_to_minutes(text)
                sess['step'] = 'bulk_collect_posts'
                await update.message.reply_text(
                    f"✅ Duration: <b>{sess['duration_minutes']} min</b>\n\nSend all posts. Click Done when ready.",
                    reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
            except ValueError:
                await update.message.reply_text("❌ Use: 2h 6h 12h 1d", reply_markup=get_duration_keyboard())
            return

        if sess['step'] == 'bulk_collect_posts':
            if "✅ Done" in text:
                if not sess.get('posts'):
                    await update.message.reply_text("❌ Send at least one post.", reply_markup=get_bulk_collection_keyboard()); return
                n    = len(sess['posts'])
                dur  = sess['duration_minutes']
                intv = dur / n if n > 1 else 0
                sess['step'] = 'bulk_confirm'
                await update.message.reply_text(
                    f"📋 <b>CONFIRM</b>\n\n📦 {n} posts  📢 {len(channel_ids)} channels\n"
                    f"⏱️ {dur} min  interval {intv:.1f} min\n\n⚠️ Click Confirm",
                    reply_markup=get_confirmation_keyboard(), parse_mode='HTML')
                return
            c = extract_content(update.message)
            if c:
                sess.setdefault('posts', []).append(c)
                await update.message.reply_text(
                    f"✅ Post #{len(sess['posts'])} added! Send more or Done.",
                    reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
            return

        if sess['step'] == 'bulk_confirm':
            if "✅ Confirm" in text:
                await _schedule_bulk(update, sess)
                user_sessions[uid] = {'mode': None, 'step': 'choose_mode'}
            else:
                await update.message.reply_text("⚠️ Click Confirm or Cancel.", reply_markup=get_confirmation_keyboard())
            return

    # ════ BATCH ══════════════════════════════
    elif sess['mode'] == 'batch':

        if sess['step'] == 'batch_choose_type':
            if "📊 Normal" in text:
                sess['batch_type'] = 'normal'
                sess['step'] = 'batch_get_start_time'
                await update.message.reply_text(
                    f"📊 <b>NORMAL BATCH</b>\n🕐 Now: <b>{get_ist_now().strftime('%H:%M:%S')} IST</b>\n\n"
                    "When should the first batch go out?",
                    reply_markup=get_exact_time_keyboard(), parse_mode='HTML')
            elif "⚡ Auto-Continuous" in text:
                sess['batch_type'] = 'autocont'
                sess['step'] = 'autocont_get_start'
                last = db.post_get_last(BOT_ID)
                if last:
                    last_ist = utc_to_ist(last['scheduled_time'])
                    hint = f"\n\n💡 Last post: <b>{last_ist.strftime('%Y-%m-%d %H:%M')} IST</b>\nYou can start after it."
                else:
                    hint = ""
                await update.message.reply_text(
                    f"⚡ <b>AUTO-CONTINUOUS BATCHES</b>\n"
                    f"🕐 Now: <b>{get_ist_now().strftime('%H:%M:%S')} IST</b>{hint}\n\n"
                    "When should the first batch go out?\n"
                    "<code>now  30m  2h  today 20:00  tomorrow 9am</code>",
                    reply_markup=get_exact_time_keyboard(), parse_mode='HTML')
            else:
                await update.message.reply_text("Choose batch type:", reply_markup=get_batch_type_keyboard())
            return

        if sess['step'] == 'batch_get_start_time':
            try:
                ist = parse_user_time_input(text)
                sess['batch_start_time_utc'] = ist_to_utc(ist)
                sess['step'] = 'batch_get_duration'
                await update.message.reply_text(
                    f"✅ Start: <b>{ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n\nTotal duration?",
                    reply_markup=get_duration_keyboard(), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=get_exact_time_keyboard())
            return

        if sess['step'] == 'batch_get_duration':
            try:
                sess['duration_minutes'] = parse_duration_to_minutes(text)
                sess['step'] = 'batch_get_batch_size'
                await update.message.reply_text(
                    f"✅ Duration: <b>{sess['duration_minutes']} min</b>\n\nPosts per batch?",
                    reply_markup=get_batch_size_keyboard(), parse_mode='HTML')
            except ValueError:
                await update.message.reply_text("❌ Use: 2h 6h 12h 1d", reply_markup=get_duration_keyboard())
            return

        if sess['step'] == 'batch_get_batch_size':
            try:
                bs = int(text.strip())
                if bs < 1: raise ValueError
                sess['batch_size'] = bs
                sess['step'] = 'batch_collect_posts'
                await update.message.reply_text(
                    f"✅ Batch size: <b>{bs}</b>\n\nSend all posts. Click Done.",
                    reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
            except ValueError:
                await update.message.reply_text("❌ Enter a number.", reply_markup=get_batch_size_keyboard())
            return

        if sess['step'] == 'batch_collect_posts':
            if "✅ Done" in text:
                if not sess.get('posts'):
                    await update.message.reply_text("❌ Send at least one post.", reply_markup=get_bulk_collection_keyboard()); return
                n  = len(sess['posts']); bs = sess['batch_size']
                nb = (n + bs - 1) // bs;  bi = sess['duration_minutes'] / nb if nb > 1 else 0
                sess['step'] = 'batch_confirm'
                await update.message.reply_text(
                    f"📋 <b>CONFIRM</b>\n\n📦 {n} posts  🎯 {bs}/batch  📊 {nb} batches\n"
                    f"📢 {len(channel_ids)} channels  ⏳ interval {bi:.1f} min\n\n⚠️ Click Confirm",
                    reply_markup=get_confirmation_keyboard(), parse_mode='HTML')
                return
            c = extract_content(update.message)
            if c:
                sess.setdefault('posts', []).append(c)
                await update.message.reply_text(f"✅ Post #{len(sess['posts'])} added!", reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
            return

        if sess['step'] == 'batch_confirm':
            if "✅ Confirm" in text:
                await _schedule_batch(update, sess)
                user_sessions[uid] = {'mode': None, 'step': 'choose_mode'}
            else:
                await update.message.reply_text("⚠️ Click Confirm or Cancel.", reply_markup=get_confirmation_keyboard())
            return

        # ════ AUTO-CONTINUOUS SUB-MODE ═══════
        if sess['step'] == 'autocont_get_start':
            try:
                ist = parse_user_time_input(text)
                sess['auto_start_time_utc'] = ist_to_utc(ist)
                sess['step'] = 'autocont_get_interval'
                await update.message.reply_text(
                    f"✅ First batch: <b>{ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n\n"
                    "How long between each batch?\n"
                    "<code>30m  1h  2h  3h  6h  12h</code>",
                    reply_markup=get_duration_keyboard(), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=get_exact_time_keyboard())
            return

        if sess['step'] == 'autocont_get_interval':
            try:
                interval = parse_duration_to_minutes(text)
                if interval < 1:
                    raise ValueError("Interval must be at least 1 minute")
                sess['interval_minutes'] = interval
                sess['step'] = 'autocont_get_batch_size'
                h, m = divmod(interval, 60)
                interval_display = f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")
                await update.message.reply_text(
                    f"✅ Interval: <b>{interval_display}</b> between batches\n\n"
                    "How many posts per batch?",
                    reply_markup=get_batch_size_keyboard(), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=get_duration_keyboard())
            return

        if sess['step'] == 'autocont_get_batch_size':
            try:
                bs = int(text.strip())
                if bs < 1:
                    raise ValueError
                sess['batch_size'] = bs
                sess['step'] = 'autocont_collect_posts'
                interval = sess['interval_minutes']
                start_ist = utc_to_ist(sess['auto_start_time_utc'])
                h, m = divmod(interval, 60)
                interval_display = f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")
                await update.message.reply_text(
                    f"✅ <b>Auto-Continuous Setup:</b>\n"
                    f"🕐 First batch: <b>{start_ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n"
                    f"⏱️ Interval: <b>{interval_display}</b>\n"
                    f"🎯 Batch size: <b>{bs} posts</b>\n\n"
                    "Now send all your posts. Click Done when finished.",
                    reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
            except ValueError:
                await update.message.reply_text("❌ Enter a number.", reply_markup=get_batch_size_keyboard())
            return

        if sess['step'] == 'autocont_collect_posts':
            if "✅ Done" in text:
                if not sess.get('posts'):
                    await update.message.reply_text(
                        "❌ Send at least one post.", reply_markup=get_bulk_collection_keyboard()); return
                n = len(sess['posts'])
                bs = sess['batch_size']
                interval = sess['interval_minutes']
                nb = (n + bs - 1) // bs
                start_ist = utc_to_ist(sess['auto_start_time_utc'])
                h, m = divmod(interval, 60)
                interval_display = f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")
                end_utc = sess['auto_start_time_utc'] + timedelta(minutes=interval * (nb - 1))
                end_ist = utc_to_ist(end_utc)
                sess['step'] = 'autocont_confirm'
                await update.message.reply_text(
                    f"📋 <b>CONFIRM AUTO-CONTINUOUS</b>\n\n"
                    f"📦 Total posts:  <b>{n}</b>\n"
                    f"🎯 Batch size:   <b>{bs}</b>\n"
                    f"📊 Batches:      <b>{nb}</b>\n"
                    f"⏱️ Interval:     <b>{interval_display}</b>\n"
                    f"🕐 First batch:  <b>{start_ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n"
                    f"🏁 Last batch:   <b>{end_ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n"
                    f"📢 Channels:     <b>{len(channel_ids)}</b>\n\n"
                    f"⚠️ Click Confirm to schedule.",
                    reply_markup=get_confirmation_keyboard(), parse_mode='HTML')
                return
            c = extract_content(update.message)
            if c:
                sess.setdefault('posts', []).append(c)
                await update.message.reply_text(
                    f"✅ Post #{len(sess['posts'])} added! Send more or Done.",
                    reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
            return

        if sess['step'] == 'autocont_confirm':
            if "✅ Confirm" in text:
                await _schedule_autocont(update, sess)
                user_sessions[uid] = {'mode': None, 'step': 'choose_mode'}
            else:
                await update.message.reply_text(
                    "⚠️ Click Confirm or Cancel.", reply_markup=get_confirmation_keyboard())
            return

    # ════ MULTI-DAY BATCH ════════════════════
    elif sess['mode'] == 'multiday':

        if sess['step'] == 'multiday_choose_type':
            if "📊 Normal" in text:
                sess['multiday_type'] = 'normal'
                sess['step'] = 'multiday_get_start_date'
                await update.message.reply_text(
                    f"📊 <b>NORMAL MULTI-DAY</b>\n"
                    f"🕐 Now: <b>{get_ist_now().strftime('%Y-%m-%d %H:%M')} IST</b>\n\n"
                    "Which day should Day 1 start?\n"
                    "<code>today  tomorrow  2026-03-15</code>",
                    reply_markup=ReplyKeyboardMarkup([
                        [KeyboardButton("today"), KeyboardButton("tomorrow")],
                        [KeyboardButton("❌ Cancel")],
                    ], resize_keyboard=True), parse_mode='HTML')
            elif "⚡ Ex-AutoContinuous" in text:
                sess['multiday_type'] = 'ex_autocont'
                sess['step'] = 'exautocont_get_first_batch'
                await update.message.reply_text(
                    f"⚡ <b>EX-AUTOCONTINUOUS MULTI-DAY</b>\n"
                    f"🕐 Now: <b>{get_ist_now().strftime('%Y-%m-%d %H:%M')} IST</b>\n\n"
                    "When should the FIRST batch go?\n"
                    "<code>20:00  2h  30m  2026-02-25 20:00  tomorrow 9am</code>",
                    reply_markup=get_exact_time_keyboard(), parse_mode='HTML')
            else:
                await update.message.reply_text("Choose type:", reply_markup=get_multiday_type_keyboard())
            return

        # ═══ NORMAL MULTI-DAY ═══
        if sess['step'] == 'multiday_get_start_date':
            try:
                text_lower = text.strip().lower()
                now_ist = get_ist_now()
                if text_lower == 'today':
                    start_date = now_ist.date()
                elif text_lower == 'tomorrow':
                    start_date = (now_ist + timedelta(days=1)).date()
                else:
                    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'):
                        try:
                            start_date = datetime.strptime(text.strip(), fmt).date()
                            break
                        except ValueError:
                            pass
                    else:
                        raise ValueError("Use: today, tomorrow, or 2026-03-15")
                start_midnight_ist = datetime.combine(start_date, datetime.min.time())
                sess['first_date_utc'] = ist_to_utc(start_midnight_ist)
                sess['step'] = 'multiday_get_window_start'
                await update.message.reply_text(
                    f"✅ Day 1: <b>{start_date.strftime('%Y-%m-%d')}</b>\n\n"
                    "What time should posts START each day? (IST)\n"
                    "<code>20:00  8pm  21:00</code>",
                    reply_markup=get_time_of_day_keyboard(), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(
                    f"❌ {e}\nUse: today, tomorrow, or 2026-03-15",
                    reply_markup=ReplyKeyboardMarkup([
                        [KeyboardButton("today"), KeyboardButton("tomorrow")],
                        [KeyboardButton("❌ Cancel")],
                    ], resize_keyboard=True))
            return

        if sess['step'] == 'multiday_get_window_start':
            try:
                h = parse_hour(text.strip())
                if not 0 <= h <= 23:
                    raise ValueError("Hour must be 0-23")
                sess['window_start_hour'] = h
                sess['step'] = 'multiday_get_window_end'
                await update.message.reply_text(
                    f"✅ Window start: <b>{h:02d}:00 IST</b>\n\n"
                    "What time should posts END each day? (IST)\n"
                    "Can be next day — e.g. <b>01:00</b> for 1 AM\n"
                    "<code>01:00  1am  00:00  23:00</code>",
                    reply_markup=ReplyKeyboardMarkup([
                        [KeyboardButton("01:00"), KeyboardButton("00:00"), KeyboardButton("23:00")],
                        [KeyboardButton("02:00"), KeyboardButton("03:00")],
                        [KeyboardButton("❌ Cancel")],
                    ], resize_keyboard=True), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=get_time_of_day_keyboard())
            return

        if sess['step'] == 'multiday_get_window_end':
            try:
                h = parse_hour(text.strip())
                if not 0 <= h <= 23:
                    raise ValueError("Hour must be 0-23")
                if h == sess['window_start_hour']:
                    raise ValueError("End time cannot be same as start time")
                sess['window_end_hour'] = h
                sess['step'] = 'multiday_get_batch_size'
                start_h = sess['window_start_hour']
                wm = (h - start_h) * 60 if h > start_h else (24 - start_h + h) * 60
                await update.message.reply_text(
                    f"✅ Window: <b>{start_h:02d}:00 - {h:02d}:00 IST</b>  ({wm} min)\n\n"
                    "How many posts per batch?",
                    reply_markup=get_batch_size_keyboard(), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=ReplyKeyboardMarkup([
                    [KeyboardButton("01:00"), KeyboardButton("00:00"), KeyboardButton("23:00")],
                    [KeyboardButton("❌ Cancel")],
                ], resize_keyboard=True))
            return

        if sess['step'] == 'multiday_get_batch_size':
            try:
                bs = int(text.strip())
                if bs < 1:
                    raise ValueError
                sess['batch_size'] = bs
                sess['step'] = 'multiday_collect_posts'
                await update.message.reply_text(
                    f"✅ Batch size: <b>{bs}</b>\n\n"
                    f"Now send all your posts.\n"
                    f"Send as many as you want - bot will auto-calculate days needed.\n\n"
                    "Click <b>Done</b> when finished.",
                    reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
            except ValueError:
                await update.message.reply_text("❌ Enter a number.", reply_markup=get_batch_size_keyboard())
            return

        if sess['step'] == 'multiday_collect_posts':
            if "✅ Done" in text:
                if not sess.get('posts'):
                    await update.message.reply_text(
                        "❌ Send at least one post.", reply_markup=get_bulk_collection_keyboard()); return
                
                n = len(sess['posts'])
                bs = sess['batch_size']
                # Auto-calculate days based on posts
                days = max(1, (n + bs - 1) // bs)  # Ceiling division
                sess['multiday_days'] = days
                sh = sess['window_start_hour']
                eh = sess['window_end_hour']
                wm = (eh - sh) * 60 if eh > sh else (24 - sh + eh) * 60
                ideal_count = bs * days
                ppd = n / days
                nb_day = (round(ppd) + bs - 1) // bs
                bi = wm / nb_day if nb_day > 1 else 0
                
                warning = ""
                if n < days:
                    await update.message.reply_text(
                        f"❌ <b>ERROR: Not enough posts!</b>\n\n"
                        f"You sent <b>{n} posts</b> for <b>{days} days</b>\n"
                        f"Need at least <b>1 post per day</b>\n\n"
                        f"💡 Send at least {days - n} more posts, then Done.",
                        reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
                    return
                
                if abs(n - ideal_count) > days:
                    if n < ideal_count - days:
                        missing = ideal_count - n
                        warning = (f"\n⚠️ <b>WARNING:</b> You sent <b>{n} posts</b> but recommended is "
                                  f"<b>{ideal_count}</b>\nMissing: <b>{missing} posts</b>\n\n"
                                  f"💡 Send {missing} more or continue anyway\n\n")
                    elif n > ideal_count + days:
                        extra = n - ideal_count
                        suggested_bs = None
                        for try_bs in [5, 10, 15, 20, 25, 30, 50, 100]:
                            if abs(n - (try_bs * days)) <= days:
                                suggested_bs = try_bs
                                break
                        warning = (f"\n⚠️ <b>WARNING:</b> You sent <b>{n} posts</b> but recommended is "
                                  f"<b>{ideal_count}</b>\nExtra: <b>{extra} posts</b>\n\n")
                        if suggested_bs:
                            warning += f"💡 Cancel → Restart with batch size <b>{suggested_bs}</b>\n\n"
                
                sess['step'] = 'multiday_confirm'
                resp = f"📋 <b>CONFIRM MULTI-DAY BATCH</b>\n\n"
                resp += f"📦 Total posts:    <b>{n}</b>\n"
                resp += f"📆 Days:           <b>{days}</b>\n"
                resp += f"📊 Posts/day:      <b>{ppd:.1f}</b>\n"
                resp += f"🎯 Batch size:     <b>{bs}</b>\n"
                resp += f"🕐 Window:         <b>{sh:02d}:00 - {eh:02d}:00 IST</b>\n"
                resp += f"⏱️ Window length:  <b>{wm} min</b>\n"
                resp += f"📊 Batches/day:    <b>~{nb_day}</b>\n"
                resp += f"⏳ Batch interval: <b>{bi:.1f} min</b>\n"
                resp += f"📢 Channels:       <b>{len(channel_ids)}</b>\n"
                resp += warning
                resp += f"⚠️ Click Confirm to schedule all {n} posts across {days} days."
                
                await update.message.reply_text(resp, reply_markup=get_confirmation_keyboard(), parse_mode='HTML')
                return
            c = extract_content(update.message)
            if c:
                sess.setdefault('posts', []).append(c)
                await update.message.reply_text(
                    f"✅ Post #{len(sess['posts'])} added! Send more or Done.",
                    reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
            return

        if sess['step'] == 'multiday_confirm':
            if "✅ Confirm" in text:
                await _schedule_multiday(update, sess)
                user_sessions[uid] = {'mode': None, 'step': 'choose_mode'}
            else:
                await update.message.reply_text(
                    "⚠️ Click Confirm or Cancel.", reply_markup=get_confirmation_keyboard())
            return


        # ═══ EX-AUTOCONTINUOUS MULTI-DAY ═══
        if sess['step'] == 'exautocont_get_first_batch':
            try:
                ist = parse_user_time_input(text)
                sess['exautocont_first_batch_utc'] = ist_to_utc(ist)
                sess['exautocont_first_batch_date'] = ist.date()
                sess['step'] = 'exautocont_get_daily_start'
                await update.message.reply_text(
                    f"✅ First batch: <b>{ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n\n"
                    "Daily window START time (for Day 2+)?\n"
                    "<code>19:00  8pm  20:00</code>",
                    reply_markup=get_time_of_day_keyboard(), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=get_exact_time_keyboard())
            return

        if sess['step'] == 'exautocont_get_daily_start':
            try:
                text_input = text.strip().lower()
                # Accept both specific time and duration
                if 'h' in text_input or 'm' in text_input:
                    # It's a duration - calculate from first batch time
                    minutes = parse_duration_to_minutes(text_input)
                    first_ist = utc_to_ist(sess['exautocont_first_batch_utc'])
                    daily_start = (first_ist + timedelta(minutes=minutes)).time()
                    sess['exautocont_daily_start_hour'] = daily_start.hour
                else:
                    # It's a specific time
                    h = parse_hour(text_input)
                    if not 0 <= h <= 23:
                        raise ValueError("Hour must be 0-23")
                    sess['exautocont_daily_start_hour'] = h
                
                sess['step'] = 'exautocont_get_daily_end'
                await update.message.reply_text(
                    f"✅ Daily start: <b>{sess['exautocont_daily_start_hour']:02d}:00 IST</b>\n\n"
                    "Daily window END (specific time or duration)?\n"
                    "<code>01:00  6h  7h30m</code>",
                    reply_markup=ReplyKeyboardMarkup([
                        [KeyboardButton("01:00"), KeyboardButton("02:00"), KeyboardButton("6h")],
                        [KeyboardButton("7h"), KeyboardButton("8h")],
                        [KeyboardButton("❌ Cancel")],
                    ], resize_keyboard=True), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=get_time_of_day_keyboard())
            return

        if sess['step'] == 'exautocont_get_daily_end':
            try:
                text_input = text.strip().lower()
                if 'h' in text_input or 'm' in text_input:
                    # Duration format
                    sess['exautocont_window_is_duration'] = True
                    minutes = parse_duration_to_minutes(text_input)
                    sess['exautocont_window_duration_minutes'] = minutes
                    h, m = divmod(minutes, 60)
                    duration_display = f"{h}h {m}m" if m else f"{h}h"
                    confirm_text = f"✅ Window duration: <b>{duration_display}</b>"
                else:
                    # Specific time format
                    sess['exautocont_window_is_duration'] = False
                    h = parse_hour(text_input)
                    if not 0 <= h <= 23:
                        raise ValueError("Hour must be 0-23")
                    sess['exautocont_daily_end_hour'] = h
                    confirm_text = f"✅ Daily end: <b>{h:02d}:00 IST</b>"
                
                sess['step'] = 'exautocont_get_batch_size'
                await update.message.reply_text(
                    f"{confirm_text}\n\n"
                    "How many posts per batch?",
                    reply_markup=get_batch_size_keyboard(), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=ReplyKeyboardMarkup([
                    [KeyboardButton("01:00"), KeyboardButton("6h"), KeyboardButton("7h")],
                    [KeyboardButton("❌ Cancel")],
                ], resize_keyboard=True))
            return

        if sess['step'] == 'exautocont_get_batch_size':
            try:
                bs = int(text.strip())
                if bs < 1:
                    raise ValueError
                sess['batch_size'] = bs
                sess['step'] = 'exautocont_get_interval'
                await update.message.reply_text(
                    f"✅ Batch size: <b>{bs}</b>\n\n"
                    "Interval between each batch?\n"
                    "<code>30m  1h  1h30m  2h</code>",
                    reply_markup=ReplyKeyboardMarkup([
                        [KeyboardButton("30m"), KeyboardButton("45m"), KeyboardButton("1h")],
                        [KeyboardButton("1h30m"), KeyboardButton("2h")],
                        [KeyboardButton("❌ Cancel")],
                    ], resize_keyboard=True), parse_mode='HTML')
            except ValueError:
                await update.message.reply_text("❌ Enter a number.", reply_markup=get_batch_size_keyboard())
            return

        if sess['step'] == 'exautocont_get_interval':
            try:
                interval = parse_duration_to_minutes(text.strip())
                if interval < 1:
                    raise ValueError("Interval must be at least 1 minute")
                sess['exautocont_interval_minutes'] = interval
                sess['step'] = 'exautocont_collect_posts'
                h, m = divmod(interval, 60)
                interval_display = f"{h}h {m}m" if m else f"{h}h"
                await update.message.reply_text(
                    f"✅ Interval: <b>{interval_display}</b>\n\n"
                    "Now send all your posts.\n"
                    "Bot will auto-calculate days needed.\n\n"
                    "Click <b>Done</b> when finished.",
                    reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=ReplyKeyboardMarkup([
                    [KeyboardButton("30m"), KeyboardButton("1h"), KeyboardButton("2h")],
                    [KeyboardButton("❌ Cancel")],
                ], resize_keyboard=True))
            return

        if sess['step'] == 'exautocont_collect_posts':
            if "✅ Done" in text:
                if not sess.get('posts'):
                    await update.message.reply_text(
                        "❌ Send at least one post.", reply_markup=get_bulk_collection_keyboard()); return
                
                n = len(sess['posts'])
                bs = sess['batch_size']
                interval = sess['exautocont_interval_minutes']
                
                # Calculate window info
                if sess['exautocont_window_is_duration']:
                    window_minutes = sess['exautocont_window_duration_minutes']
                    h, m = divmod(window_minutes, 60)
                    window_display = f"{h}h {m}m" if m else f"{h}h"
                else:
                    start_h = sess['exautocont_daily_start_hour']
                    end_h = sess['exautocont_daily_end_hour']
                    if end_h > start_h:
                        window_minutes = (end_h - start_h) * 60
                    else:
                        window_minutes = (24 - start_h + end_h) * 60
                    window_display = f"{start_h:02d}:00 - {end_h:02d}:00"
                
                # Calculate batches per day
                batches_per_day = int(window_minutes / interval)
                posts_per_day = batches_per_day * bs
                
                # Estimate days
                days_estimate = max(1, (n + posts_per_day - 1) // posts_per_day)
                
                sess['step'] = 'exautocont_confirm'
                h, m = divmod(interval, 60)
                interval_display = f"{h}h {m}m" if m else f"{h}h"
                
                first_ist = utc_to_ist(sess['exautocont_first_batch_utc'])
                
                await update.message.reply_text(
                    f"📋 <b>CONFIRM EX-AUTOCONTINUOUS</b>\n\n"
                    f"📦 Total posts:     <b>{n}</b>\n"
                    f"🎯 Batch size:      <b>{bs}</b>\n"
                    f"⏱️ Interval:        <b>{interval_display}</b>\n"
                    f"🕐 First batch:     <b>{first_ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n"
                    f"📅 Daily window:    <b>{window_display}</b>\n"
                    f"📊 Est. days:       <b>~{days_estimate}</b>\n"
                    f"📢 Channels:        <b>{len(channel_ids)}</b>\n\n"
                    f"⚠️ Click Confirm to schedule.",
                    reply_markup=get_confirmation_keyboard(), parse_mode='HTML')
                return
            c = extract_content(update.message)
            if c:
                sess.setdefault('posts', []).append(c)
                await update.message.reply_text(
                    f"✅ Post #{len(sess['posts'])} added! Send more or Done.",
                    reply_markup=get_bulk_collection_keyboard(), parse_mode='HTML')
            return

        if sess['step'] == 'exautocont_confirm':
            if "✅ Confirm" in text:
                await _schedule_exautocont(update, sess)
                user_sessions[uid] = {'mode': None, 'step': 'choose_mode'}
            else:
                await update.message.reply_text(
                    "⚠️ Click Confirm or Cancel.", reply_markup=get_confirmation_keyboard())
            return

    # ════ EXACT ══════════════════════════════
    # ════ EXACT ══════════════════════════════

        if sess['step'] == 'exact_get_time':
            try:
                ist = parse_user_time_input(text)
                sess['scheduled_time_utc'] = ist_to_utc(ist)
                sess['step'] = 'exact_get_content'
                await update.message.reply_text(
                    f"✅ Time: <b>{ist.strftime('%Y-%m-%d %H:%M:%S')} IST</b>\n\nSend the content to post.",
                    reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Cancel")]], resize_keyboard=True),
                    parse_mode='HTML')
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=get_exact_time_keyboard())
            return

        if sess['step'] == 'exact_get_content':
            c = extract_content(update.message)
            if not c:
                await update.message.reply_text("❌ Send text, photo, video or document."); return
            sess['content'] = c
            sess['step']    = 'exact_confirm'
            ist  = utc_to_ist(sess['scheduled_time_utc'])
            mins = int((sess['scheduled_time_utc'] - utc_now()).total_seconds() / 60)
            prev = c.get('message', '')[:50] or f"[{c.get('media_type','media')}]"
            await update.message.reply_text(
                f"📋 <b>CONFIRM</b>\n\n📅 {ist.strftime('%Y-%m-%d %H:%M:%S')} IST (in {mins} min)\n"
                f"📢 {len(channel_ids)} channels\n📝 {prev}\n\n⚠️ Click Confirm",
                reply_markup=get_confirmation_keyboard(), parse_mode='HTML')
            return

        if sess['step'] == 'exact_confirm':
            if "✅ Confirm" in text:
                c = sess['content']
                pid = db.post_insert(BOT_ID, sess['scheduled_time_utc'], len(channel_ids),
                                     c.get('message'), c.get('media_type'),
                                     c.get('media_file_id'), c.get('caption'))
                ist = utc_to_ist(sess['scheduled_time_utc'])
                await update.message.reply_text(
                    f"✅ <b>SCHEDULED!</b>\n🆔 #{pid}  📅 {ist.strftime('%Y-%m-%d %H:%M:%S')} IST\n"
                    f"📢 {len(channel_ids)} channels",
                    reply_markup=get_mode_keyboard(), parse_mode='HTML')
                user_sessions[uid] = {'mode': None, 'step': 'choose_mode'}
            else:
                await update.message.reply_text("⚠️ Click Confirm or Cancel.", reply_markup=get_confirmation_keyboard())
            return

    # ════ DURATION ════════════════════════════
    elif sess['mode'] == 'duration':

        if sess['step'] == 'duration_get_time':
            try:
                ist = parse_user_time_input(text)
                sess['scheduled_time_utc'] = ist_to_utc(ist)
                sess['step'] = 'duration_get_content'
                await update.message.reply_text(
                    f"✅ Will post at: <b>{ist.strftime('%Y-%m-%d %H:%M:%S')} IST</b>\n\nSend the content.",
                    reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Cancel")]], resize_keyboard=True),
                    parse_mode='HTML')
            except ValueError:
                await update.message.reply_text("❌ Use: 5m 30m 2h 1d now", reply_markup=get_quick_time_keyboard())
            return

        if sess['step'] == 'duration_get_content':
            c = extract_content(update.message)
            if not c:
                await update.message.reply_text("❌ Send text, photo, video or document."); return
            sess['content'] = c
            sess['step']    = 'duration_confirm'
            ist  = utc_to_ist(sess['scheduled_time_utc'])
            mins = int((sess['scheduled_time_utc'] - utc_now()).total_seconds() / 60)
            prev = c.get('message', '')[:50] or f"[{c.get('media_type','media')}]"
            await update.message.reply_text(
                f"📋 <b>CONFIRM</b>\n\n⏱️ In {mins} min  ({ist.strftime('%H:%M:%S')} IST)\n"
                f"📢 {len(channel_ids)} channels\n📝 {prev}\n\n⚠️ Click Confirm",
                reply_markup=get_confirmation_keyboard(), parse_mode='HTML')
            return

        if sess['step'] == 'duration_confirm':
            if "✅ Confirm" in text:
                c = sess['content']
                pid = db.post_insert(BOT_ID, sess['scheduled_time_utc'], len(channel_ids),
                                     c.get('message'), c.get('media_type'),
                                     c.get('media_file_id'), c.get('caption'))
                mins = int((sess['scheduled_time_utc'] - utc_now()).total_seconds() / 60)
                ist  = utc_to_ist(sess['scheduled_time_utc'])
                await update.message.reply_text(
                    f"✅ <b>SCHEDULED!</b>\n🆔 #{pid}  ⏱️ In {mins} min\n"
                    f"📅 {ist.strftime('%H:%M:%S')} IST  📢 {len(channel_ids)} channels",
                    reply_markup=get_mode_keyboard(), parse_mode='HTML')
                user_sessions[uid] = {'mode': None, 'step': 'choose_mode'}
            else:
                await update.message.reply_text("⚠️ Click Confirm or Cancel.", reply_markup=get_confirmation_keyboard())
            return

    await update.message.reply_text("Choose a mode:", reply_markup=get_mode_keyboard())


# ─────────────────────────────────────────────
# Background tasks
# ─────────────────────────────────────────────
async def background_poster(application: Application):
    bot  = application.bot
    tick = 0
    while True:
        try:
            await process_due_posts(bot)
            tick += 1
            if tick >= 2:
                cleanup_posted_content()
                tick = 0
        except Exception as e:
            logger.error(f"Background error: {e}", exc_info=True)
        await asyncio.sleep(15)

async def post_init(application: Application):
    asyncio.create_task(background_poster(application))


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────


async def lastpost_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    last = db.post_get_last(BOT_ID)
    if not last:
        await update.message.reply_text("No posts sent yet.", reply_markup=get_mode_keyboard())
        return
    
    ist = utc_to_ist(last['scheduled_time'])
    posted_ist = utc_to_ist(last['posted_at']) if last.get('posted_at') else None
    
    resp = f"📮 <b>LAST SENT POST</b>\n\n"
    resp += f"🆔 ID: <b>#{last['id']}</b>\n"
    resp += f"🕐 Scheduled: <b>{ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n"
    if posted_ist:
        resp += f"✅ Sent at: <b>{posted_ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n"
    resp += f"📢 Channels: <b>{last['successful_posts']}/{last['total_channels']}</b>\n\n"
    
    if last.get('message'):
        preview = last['message'][:100] + "..." if len(last['message']) > 100 else last['message']
        resp += f"<b>Content:</b>\n{preview}"
    elif last.get('media_type'):
        resp += f"<b>Type:</b> {last['media_type']}"
        if last.get('caption'):
            preview = last['caption'][:100] + "..." if len(last['caption']) > 100 else last['caption']
            resp += f"\n{preview}"
    
    await update.message.reply_text(resp, reply_markup=get_mode_keyboard(), parse_mode='HTML')

def main():
    global BOT_ID, ADMIN_ID, AUTO_CLEANUP_MINUTES, posting_lock

    BOT_TOKEN    = os.environ.get('BOT_TOKEN', '').strip()
    ADMIN_ID_STR = os.environ.get('ADMIN_ID', '').strip()
    DATABASE_URL = os.environ.get('DATABASE_URL', '').strip()

    if not BOT_TOKEN:    logger.error("❌ BOT_TOKEN not set");    sys.exit(1)
    if not ADMIN_ID_STR: logger.error("❌ ADMIN_ID not set");     sys.exit(1)
    if not DATABASE_URL: logger.error("❌ DATABASE_URL not set"); sys.exit(1)

    ADMIN_ID             = int(ADMIN_ID_STR)
    AUTO_CLEANUP_MINUTES = int(os.environ.get('AUTO_CLEANUP_MINUTES', '30'))
    posting_lock         = asyncio.Lock()

    CHANNEL_IDS = [c.strip() for c in os.environ.get('CHANNEL_IDS', '').split(',') if c.strip()]

    db.init_pool(DATABASE_URL, minconn=2, maxconn=10)
    db.bootstrap_schema()

    BOT_ID = db.make_bot_id(BOT_TOKEN)
    db.register_tenant(BOT_ID, 'scheduler')

    for cid in CHANNEL_IDS:
        db.channel_add(BOT_ID, cid)
    reload_channels()

    from telegram.request import HTTPXRequest
    request = HTTPXRequest(connection_pool_size=20,
                           connect_timeout=90.0, read_timeout=90.0,
                           write_timeout=90.0,  pool_timeout=90.0)

    app = (Application.builder()
           .token(BOT_TOKEN)
           .request(request)
           .post_init(post_init)
           .build())

    app.add_handler(CommandHandler("start",          start))
    app.add_handler(CommandHandler("list",           list_posts))
    app.add_handler(CommandHandler("stats",          stats_command))
    app.add_handler(CommandHandler("channels",       channels_command))
    app.add_handler(CommandHandler("addchannel",     add_channel_command))
    app.add_handler(CommandHandler("removechannel",  remove_channel_command))
    app.add_handler(CommandHandler("delete",         delete_post_cmd))
    app.add_handler(CommandHandler("cancel",         cancel))
    app.add_handler(CommandHandler("reset",          reset_command))
    app.add_handler(CommandHandler("exportchannels", export_channels_command))
    app.add_handler(CommandHandler("backup",         backup_posts_command))
    app.add_handler(MessageHandler(filters.ALL,      handle_message))

    logger.info("=" * 60)
    logger.info("✅  SCHEDULER BOT  (PostgreSQL / multi-tenant)")
    logger.info(f"🤖  Bot tenant ID  : {BOT_ID}")
    logger.info(f"👤  Admin ID       : {ADMIN_ID}")
    logger.info(f"📢  Channels       : {len(channel_ids)}")
    logger.info(f"🧹  Auto-cleanup   : {AUTO_CLEANUP_MINUTES} min")
    logger.info("=" * 60)

    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
