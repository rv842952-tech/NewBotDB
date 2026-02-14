"""
forwarder_bot.py  ─  Auto-Copy Bot (multi-tenant, PostgreSQL)
──────────────────────────────────────────────────────────────
Copies every message from a MASTER_CHANNEL to all target
channels managed by THIS bot instance.

• Shares the same PostgreSQL database with the scheduler bot.
• Full tenant isolation: bot_id column on every row.
• One bot's crash / data corruption never touches another bot.

ENV VARS
────────
  FORWARD_BOT_TOKEN  — Telegram bot token  (unique per instance)
  MASTER_CHANNEL     — Integer channel ID to monitor
  ADMIN_ID           — Your Telegram user ID
  DATABASE_URL       — Shared PostgreSQL URL
  BATCH_SIZE         — Channels per parallel batch  (default 20)
  TARGET_CHANNELS    — Optional comma-separated channel IDs (bootstrap only)
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime

import db  # shared DB layer
from telegram import Update
from telegram.error import NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import (Application, CommandHandler, ContextTypes,
                          MessageHandler, filters)
from telegram.request import HTTPXRequest

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

logging.basicConfig(
    format='%(asctime)s [%(name)s] %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('forwarder_bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config from env
# ─────────────────────────────────────────────
FORWARD_BOT_TOKEN = os.environ.get('FORWARD_BOT_TOKEN', '').strip()
MASTER_CHANNEL    = os.environ.get('MASTER_CHANNEL', '').strip()
ADMIN_ID          = os.environ.get('ADMIN_ID', '').strip()
DATABASE_URL      = os.environ.get('DATABASE_URL', '').strip()
BATCH_SIZE        = int(os.environ.get('BATCH_SIZE', '30'))

# Derived once at startup
BOT_ID: str = ''           # set in main()
TARGET_CHANNELS: list[str] = []   # in-memory cache, reloaded on demand

# In-process stats (reset on restart; history lives in forward_log table)
_runtime_stats = {
    'messages_processed': 0,
    'total_forwards': 0,
    'successful_forwards': 0,
    'failed_forwards': 0,
    'last_forward_time': None,
    'restarts': 0,
}


# ─────────────────────────────────────────────
# Channel cache helpers
# ─────────────────────────────────────────────
def reload_channels():
    global TARGET_CHANNELS
    TARGET_CHANNELS = db.channel_list_active(BOT_ID)
    logger.info(f"🔄 Channels reloaded: {len(TARGET_CHANNELS)} active")


def _migrate_env_channels():
    """One-time bootstrap: import channels from env var into the DB."""
    env_raw = os.environ.get('TARGET_CHANNELS', '')
    ids = [c.strip() for c in env_raw.split(',') if c.strip()]
    if ids:
        logger.info(f"📄 Bootstrapping {len(ids)} channels from env…")
        for cid in ids:
            db.channel_add(BOT_ID, cid, "Imported from env")
        logger.info("✅ Bootstrap complete")


# ─────────────────────────────────────────────
# Admin gate
# ─────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return ADMIN_ID and str(user_id) == ADMIN_ID


# ─────────────────────────────────────────────
# Core copy logic
# ─────────────────────────────────────────────
async def _copy_to_one(bot, message, channel_id: str, retries: int = 2) -> bool:
    """
    Copy a single message to one channel — no 'Forwarded from' label.
    Returns True on success, False on permanent failure.
    """
    for attempt in range(retries):
        try:
            kw = dict(read_timeout=15, write_timeout=15, connect_timeout=10)

            if message.text:
                await bot.send_message(channel_id, message.text,
                                       entities=message.entities, **kw)
            elif message.photo:
                await bot.send_photo(channel_id, message.photo[-1].file_id,
                                     caption=message.caption,
                                     caption_entities=message.caption_entities, **kw)
            elif message.video:
                await bot.send_video(channel_id, message.video.file_id,
                                     caption=message.caption,
                                     caption_entities=message.caption_entities,
                                     duration=message.video.duration,
                                     width=message.video.width,
                                     height=message.video.height, **kw)
            elif message.document:
                await bot.send_document(channel_id, message.document.file_id,
                                        caption=message.caption,
                                        caption_entities=message.caption_entities, **kw)
            elif message.audio:
                await bot.send_audio(channel_id, message.audio.file_id,
                                     caption=message.caption,
                                     caption_entities=message.caption_entities, **kw)
            elif message.voice:
                await bot.send_voice(channel_id, message.voice.file_id,
                                     caption=message.caption,
                                     caption_entities=message.caption_entities, **kw)
            elif message.video_note:
                await bot.send_video_note(channel_id, message.video_note.file_id, **kw)
            elif message.sticker:
                await bot.send_sticker(channel_id, message.sticker.file_id, **kw)
            elif message.animation:
                await bot.send_animation(channel_id, message.animation.file_id,
                                         caption=message.caption,
                                         caption_entities=message.caption_entities, **kw)
            elif message.poll:
                await bot.send_poll(channel_id,
                                    question=message.poll.question,
                                    options=[o.text for o in message.poll.options],
                                    is_anonymous=message.poll.is_anonymous,
                                    type=message.poll.type,
                                    allows_multiple_answers=message.poll.allows_multiple_answers,
                                    **kw)
            elif message.location:
                await bot.send_location(channel_id,
                                        latitude=message.location.latitude,
                                        longitude=message.location.longitude, **kw)
            elif message.contact:
                await bot.send_contact(channel_id,
                                       phone_number=message.contact.phone_number,
                                       first_name=message.contact.first_name,
                                       last_name=message.contact.last_name, **kw)
            else:
                logger.warning(f"⚠️ Unsupported message type → {channel_id}")
                return False

            # Update per-channel counter (non-critical — don't crash on failure)
            try:
                db.channel_increment_forward(BOT_ID, channel_id)
            except Exception as e:
                logger.warning(f"⚠️ Counter update failed for {channel_id}: {e}")

            return True

        except RetryAfter as e:
            wait = e.retry_after + 1
            logger.warning(f"⏳ Rate-limit for {channel_id} — waiting {wait}s")
            await asyncio.sleep(wait)

        except TimedOut:
            logger.warning(f"⏱️ Timeout → {channel_id} (attempt {attempt+1}/{retries})")
            await asyncio.sleep(1)

        except TelegramError as e:
            perm = ('chat not found', 'bot was kicked', 'not a member',
                    'have no rights', 'forbidden')
            if any(p in str(e).lower() for p in perm):
                logger.error(f"❌ Permanent error → {channel_id}: {e}")
                return False
            logger.error(f"❌ TelegramError → {channel_id} (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"❌ Unexpected → {channel_id}: {e}")
            return False

    logger.error(f"❌ Gave up on {channel_id} after {retries} attempts")
    return False


async def forward_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler: triggered by every new post in MASTER_CHANNEL.
    Copies it in parallel batches to all TARGET_CHANNELS.
    """
    global _runtime_stats, BATCH_SIZE

    message = update.channel_post
    if not message:
        return

    reload_channels()

    if not TARGET_CHANNELS:
        logger.warning("⚠️ No active channels — skipping forward")
        return

    # Determine message type label for logging
    type_map = [
        (message.photo,      'photo'),
        (message.video,      'video'),
        (message.document,   'document'),
        (message.audio,      'audio'),
        (message.voice,      'voice'),
        (message.video_note, 'video_note'),
        (message.sticker,    'sticker'),
        (message.animation,  'animation'),
        (message.poll,       'poll'),
        (message.location,   'location'),
        (message.contact,    'contact'),
    ]
    msg_type = next((t for attr, t in type_map if attr), 'text')

    logger.info("=" * 60)
    logger.info(f"📨 {msg_type.upper()} from master → copying to {len(TARGET_CHANNELS)} channels")
    logger.info("=" * 60)

    t0 = datetime.now()
    successful = 0
    failed = 0
    total_batches = (len(TARGET_CHANNELS) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(TARGET_CHANNELS), BATCH_SIZE):
        batch = TARGET_CHANNELS[i:i + BATCH_SIZE]
        bn = i // BATCH_SIZE + 1
        logger.info(f"🔄 Batch {bn}/{total_batches} ({len(batch)} channels)…")

        results = await asyncio.gather(
            *[_copy_to_one(context.bot, message, ch) for ch in batch],
            return_exceptions=True
        )

        ok = sum(1 for r in results if r is True)
        successful += ok
        failed += len(batch) - ok
        logger.info(f"   ✅ {ok}  ❌ {len(batch)-ok}")

        if i + BATCH_SIZE < len(TARGET_CHANNELS):
            await asyncio.sleep(0.5)

    duration = (datetime.now() - t0).total_seconds()

    # Persist to forward_log (tenant-isolated)
    try:
        db.fwdlog_insert(BOT_ID, message.message_id, msg_type,
                         len(TARGET_CHANNELS), successful, failed, duration)
    except Exception as e:
        logger.warning(f"⚠️ Could not write forward_log: {e}")

    # Update in-process counters
    _runtime_stats['messages_processed'] += 1
    _runtime_stats['total_forwards']     += len(TARGET_CHANNELS)
    _runtime_stats['successful_forwards'] += successful
    _runtime_stats['failed_forwards']    += failed
    _runtime_stats['last_forward_time']  = datetime.now()

    rate = successful / len(TARGET_CHANNELS) * 100
    spd  = len(TARGET_CHANNELS) / duration if duration > 0 else 0
    logger.info(f"✅ Done: {successful}/{len(TARGET_CHANNELS)} ({rate:.1f}%) in {duration:.2f}s ({spd:.1f}/s)")

    # Admin alert on high failure rate
    if ADMIN_ID and failed / len(TARGET_CHANNELS) > 0.3:
        try:
            await context.bot.send_message(
                int(ADMIN_ID),
                f"⚠️ <b>HIGH FAILURE RATE</b>\n\n"
                f"✅ {successful} / {len(TARGET_CHANNELS)}  ❌ {failed}\n"
                f"📉 {(failed/len(TARGET_CHANNELS)*100):.1f}% failed\n"
                f"⏱️ {duration:.2f}s",
                parse_mode='HTML'
            )
        except Exception:
            pass


# ─────────────────────────────────────────────
# Command handlers
# ─────────────────────────────────────────────
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_admin(update.effective_user.id):
        return

    db_stats = db.fwdlog_stats(BOT_ID)
    ch_count = len(db.channel_list_active(BOT_ID))
    total    = db_stats['total_forwards'] or 0
    ok       = db_stats['successful_forwards'] or 0
    rate     = (ok / total * 100) if total else 0

    last = "Never"
    lft  = db_stats['last_forward_time']
    if lft:
        diff = int((datetime.now(lft.tzinfo) - lft).total_seconds())
        last = (f"{diff}s ago" if diff < 60
                else f"{diff//60}m ago" if diff < 3600
                else f"{diff//3600}h ago")

    await update.message.reply_text(
        f"🤖 <b>Auto-Copy Bot</b>\n\n"
        f"🤖 Bot ID: <code>{BOT_ID}</code>\n"
        f"📡 Master Channel: <code>{MASTER_CHANNEL}</code>\n"
        f"📤 Active Channels: <b>{ch_count}</b>\n"
        f"📨 Messages Processed: <b>{db_stats['messages_processed'] or 0}</b>\n"
        f"📊 Total Copies: <b>{total}</b>  ({rate:.1f}% success)\n"
        f"⏰ Last Copy: {last}\n"
        f"⚙️ Batch Size: <b>{BATCH_SIZE}</b>\n"
        f"🔒 Mode: <b>COPY MODE</b> — no 'Forwarded from'\n\n"
        f"<b>Commands:</b>\n"
        f"/addchannel &lt;id&gt; [name]\n"
        f"/removechannel &lt;id&gt;\n"
        f"/listchannels\n"
        f"/stats\n"
        f"/test\n"
        f"/reload\n"
        f"/setbatch &lt;n&gt;\n"
        f"/exportchannels",
        parse_mode='HTML'
    )


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_admin(update.effective_user.id):
        return

    s    = db.fwdlog_stats(BOT_ID)
    total = s['total_forwards'] or 0
    ok    = s['successful_forwards'] or 0
    msgs  = s['messages_processed'] or 0
    rate  = (ok / total * 100) if total else 0
    avg   = (total / msgs) if msgs else 0

    last = "Never"
    lft  = s['last_forward_time']
    if lft:
        diff = int((datetime.now(lft.tzinfo) - lft).total_seconds())
        last = (f"{diff}s ago" if diff < 60
                else f"{diff//60}m ago" if diff < 3600
                else f"{diff//3600}h ago")

    await update.message.reply_text(
        f"📊 <b>STATISTICS</b>\n\n"
        f"📤 Active Channels: <b>{len(TARGET_CHANNELS)}</b>\n"
        f"📨 Messages Processed: <b>{msgs}</b>\n"
        f"📊 Total Copies: <b>{total}</b>\n"
        f"✅ Successful: <b>{ok}</b>  ({rate:.1f}%)\n"
        f"❌ Failed: <b>{s['failed_forwards'] or 0}</b>\n"
        f"📉 Avg Channels/Message: <b>{avg:.1f}</b>\n"
        f"⏰ Last Copy: {last}\n"
        f"⚙️ Batch Size: <b>{BATCH_SIZE}</b>",
        parse_mode='HTML'
    )


async def add_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized"); return

    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b>\n<code>/addchannel -1001234567890 Name</code>",
            parse_mode='HTML'); return

    cid  = context.args[0].strip()
    name = " ".join(context.args[1:]) or "Unnamed"

    if not cid.startswith('-100'):
        await update.message.reply_text(
            "❌ Channel ID must start with <code>-100</code>", parse_mode='HTML')
        return

    db.channel_add(BOT_ID, cid, name)
    reload_channels()
    await update.message.reply_text(
        f"✅ <b>Channel Added!</b>\n<code>{cid}</code>  {name}\n"
        f"Active: <b>{len(TARGET_CHANNELS)}</b>",
        parse_mode='HTML'
    )


async def remove_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized"); return

    if not context.args:
        await update.message.reply_text(
            "❌ Usage: <code>/removechannel -1001234567890</code>", parse_mode='HTML')
        return

    cid = context.args[0].strip()
    if db.channel_remove(BOT_ID, cid):
        reload_channels()
        await update.message.reply_text(
            f"✅ Removed <code>{cid}</code>  Remaining: <b>{len(TARGET_CHANNELS)}</b>",
            parse_mode='HTML')
    else:
        await update.message.reply_text(
            f"❌ Not found: <code>{cid}</code>", parse_mode='HTML')


async def list_channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_admin(update.effective_user.id):
        return

    channels = db.channel_list_all(BOT_ID)
    if not channels:
        await update.message.reply_text("📋 No channels. /addchannel to add."); return

    active = sum(1 for c in channels if c['active'])
    resp = f"📋 <b>CHANNELS ({len(channels)} total)</b>\n\n"
    for ch in channels[:25]:
        icon = "✅" if ch['active'] else "❌"
        name = ch['channel_name'] or "Unnamed"
        fwd  = ch['total_forwards'] or 0
        resp += f"{icon} <code>{ch['channel_id']}</code>\n"
        resp += f"   📝 {name}  |  📊 {fwd} copies\n\n"
    if len(channels) > 25:
        resp += f"<i>…and {len(channels)-25} more</i>\n\n"
    resp += f"<b>Active:</b> {active}  <b>Inactive:</b> {len(channels)-active}"
    await update.message.reply_text(resp, parse_mode='HTML')


async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        f"✅ <b>Bot is online!</b>\n\n"
        f"📡 Monitoring: <code>{MASTER_CHANNEL}</code>\n"
        f"📤 Active Channels: <b>{len(TARGET_CHANNELS)}</b>\n"
        f"⚙️ Batch Size: <b>{BATCH_SIZE}</b>\n"
        f"🔒 COPY MODE — no forwarding label",
        parse_mode='HTML'
    )


async def reload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_admin(update.effective_user.id):
        return
    before = len(TARGET_CHANNELS)
    reload_channels()
    await update.message.reply_text(
        f"🔄 <b>Reloaded!</b>  Before: {before}  After: {len(TARGET_CHANNELS)}",
        parse_mode='HTML'
    )


async def setbatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global BATCH_SIZE
    if not update.effective_user or not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text(
            f"⚙️ Current batch size: <b>{BATCH_SIZE}</b>\n"
            f"Usage: <code>/setbatch 20</code>", parse_mode='HTML')
        return
    try:
        n = int(context.args[0])
        if not 1 <= n <= 50:
            raise ValueError
        BATCH_SIZE = n
        await update.message.reply_text(f"✅ Batch size → <b>{BATCH_SIZE}</b>", parse_mode='HTML')
    except ValueError:
        await update.message.reply_text("❌ Must be 1–50")


async def export_channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_admin(update.effective_user.id):
        return
    channels = db.channel_list_all(BOT_ID)
    if not channels:
        await update.message.reply_text("No channels to export."); return

    active = [ch for ch in channels if ch['active']]
    if not active:
        await update.message.reply_text("No active channels to export."); return

    # Header message
    await update.message.reply_text(
        f"📖 <b>CHANNEL BACKUP</b>\n\n"
        f"Sending <b>{len(active)}</b> channels one by one.\n"
        f"Each message is ready to forward or copy-paste.",
        parse_mode='HTML'
    )

    # One message per channel — plain command, easy to forward or re-send directly
    for ch in active:
        name = ch['channel_name'] or ''
        cmd  = f"/addchannel {ch['channel_id']}"
        if name and name != 'Unnamed':
            cmd += f" {name}"
        await update.message.reply_text(cmd)
        await asyncio.sleep(0.05)

    await update.message.reply_text(
        f"✅ Done — {len(active)} channels exported.",
        parse_mode='HTML'
    )


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"❌ Unhandled error: {context.error}", exc_info=context.error)


# ─────────────────────────────────────────────
# Heartbeat
# ─────────────────────────────────────────────
async def heartbeat():
    while True:
        await asyncio.sleep(300)
        logger.info(
            f"💓 Heartbeat | channels={len(TARGET_CHANNELS)} "
            f"processed={_runtime_stats['messages_processed']}"
        )


async def post_init(application: Application):
    asyncio.create_task(heartbeat())


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    global BOT_ID, BATCH_SIZE

    # ── Validate env ──────────────────────────
    if not FORWARD_BOT_TOKEN:
        logger.error("❌ FORWARD_BOT_TOKEN not set"); sys.exit(1)
    if not MASTER_CHANNEL:
        logger.error("❌ MASTER_CHANNEL not set"); sys.exit(1)
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL not set"); sys.exit(1)
    if not ADMIN_ID:
        logger.warning("⚠️ ADMIN_ID not set — commands disabled")

    try:
        master_id = int(MASTER_CHANNEL)
    except ValueError:
        logger.error(f"❌ MASTER_CHANNEL must be an integer: {MASTER_CHANNEL}")
        sys.exit(1)

    # ── DB init ──────────────────────────────
    db.init_pool(DATABASE_URL, minconn=2, maxconn=8)
    db.bootstrap_schema()

    BOT_ID = db.make_bot_id(FORWARD_BOT_TOKEN)
    db.register_tenant(BOT_ID, 'forwarder')

    # ── Seed channels from env (once) ────────
    _migrate_env_channels()
    reload_channels()

    # ── Build app ────────────────────────────
    request = HTTPXRequest(
        connection_pool_size=8,
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=30.0,
    )
    app = (Application.builder()
           .token(FORWARD_BOT_TOKEN)
           .request(request)
           .post_init(post_init)
           .build())

    app.add_handler(CommandHandler("start",          start_command))
    app.add_handler(CommandHandler("addchannel",     add_channel_command))
    app.add_handler(CommandHandler("removechannel",  remove_channel_command))
    app.add_handler(CommandHandler("listchannels",   list_channels_command))
    app.add_handler(CommandHandler("stats",          stats_command))
    app.add_handler(CommandHandler("test",           test_command))
    app.add_handler(CommandHandler("reload",         reload_command))
    app.add_handler(CommandHandler("setbatch",       setbatch_command))
    app.add_handler(CommandHandler("exportchannels", export_channels_command))
    app.add_handler(MessageHandler(
        filters.Chat(chat_id=master_id) & filters.ALL,
        forward_message
    ))
    app.add_error_handler(error_handler)

    logger.info("=" * 60)
    logger.info("🚀  AUTO-COPY BOT  (PostgreSQL / multi-tenant)")
    logger.info(f"🤖  Bot tenant ID  : {BOT_ID}")
    logger.info(f"📡  Master Channel : {MASTER_CHANNEL}")
    logger.info(f"📤  Active Channels: {len(TARGET_CHANNELS)}")
    logger.info(f"👤  Admin ID       : {ADMIN_ID or 'not set'}")
    logger.info(f"⚙️  Batch Size     : {BATCH_SIZE}")
    logger.info(f"🔒  Mode           : COPY (no forwarding label)")
    logger.info("=" * 60)

    # ── Auto-restart loop ────────────────────
    while True:
        try:
            app.run_polling(
                allowed_updates=['channel_post', 'message'],
                drop_pending_updates=True,
            )
            break
        except TimedOut:
            _runtime_stats['restarts'] += 1
            logger.warning(f"⏱️ Timeout — restarting (#{_runtime_stats['restarts']})…")
            time.sleep(5)
        except NetworkError as e:
            _runtime_stats['restarts'] += 1
            logger.warning(f"🌐 NetworkError: {e} — restarting…")
            time.sleep(10)
        except KeyboardInterrupt:
            logger.info("🛑 Shutdown")
            break
        except Exception as e:
            _runtime_stats['restarts'] += 1
            logger.error(f"❌ Unexpected: {e} — restarting…")
            time.sleep(15)
            if _runtime_stats['restarts'] > 10:
                logger.error("❌ Too many restarts — exiting"); sys.exit(1)


if __name__ == "__main__":
    main()
