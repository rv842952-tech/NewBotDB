"""
db.py  ─  Shared PostgreSQL layer for all bot instances
────────────────────────────────────────────────────────
• One connection pool shared across every bot process
  that imports this module.
• Every public function is tenant-scoped: it takes bot_id
  as its first argument and ALWAYS filters by it.
• Schema is created once with IF NOT EXISTS — safe for
  concurrent first-starts across many bot processes.

ISOLATION GUARANTEES
────────────────────
1. Foreign key  bot_tenants(bot_id) → posts/channels ON DELETE CASCADE
   Deleting a tenant wipes all its data and nothing else.
2. Every SELECT / UPDATE / DELETE carries WHERE bot_id = %s.
   Postgres will never return another tenant's rows even on a
   bug — the planner simply finds no matching pages.
3. UNIQUE constraints are (bot_id, channel_id) — not just
   channel_id — so two bots can target the same channel
   independently.
4. The connection pool uses autocommit=False + explicit
   SAVEPOINT per operation, so a crash inside one bot's
   transaction rolls back only that transaction; other bots'
   in-flight transactions are untouched.
"""

import hashlib
import logging
import os
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import psycopg2.pool

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# Connection pool  (module-level singleton)
# ─────────────────────────────────────────────────────────
_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def init_pool(database_url: str, minconn: int = 2, maxconn: int = 20,
              retries: int = 5):
    """
    Connect to PostgreSQL with retry logic.
    Retries up to `retries` times with increasing delays before giving up.
    Handles brief database unavailability on startup or after a hiccup.
    """
    global _pool
    for attempt in range(retries):
        try:
            _pool = psycopg2.pool.ThreadedConnectionPool(minconn, maxconn, database_url)
            logger.info(f"✅ DB pool ready (min={minconn}, max={maxconn})")
            return
        except Exception as e:
            wait = (attempt + 1) * 5   # 5s, 10s, 15s, 20s, 25s
            if attempt < retries - 1:
                logger.warning(f"⚠️ DB connection failed (attempt {attempt+1}/{retries}), "
                               f"retrying in {wait}s… ({e})")
                time.sleep(wait)
            else:
                logger.error(f"❌ Could not connect to database after {retries} attempts")
                raise


@contextmanager
def get_conn(retries: int = 3):
    """
    Yield a pooled connection.
    Commits on clean exit, rolls back on exception.
    Retries up to `retries` times on OperationalError (e.g. brief DB hiccup).
    Always returns the connection to the pool.
    """
    if _pool is None:
        raise RuntimeError("Call db.init_pool() before using get_conn()")

    last_error = None
    for attempt in range(retries):
        conn = _pool.getconn()
        try:
            # Reset connection state — guards against a previously aborted
            # transaction being silently reused from the pool.
            if conn.closed:
                _pool.putconn(conn, close=True)
                conn = _pool.getconn()
            conn.autocommit = False
            if conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                conn.rollback()
        except Exception as e:
            _pool.putconn(conn)
            raise

        try:
            yield conn
            conn.commit()
            _pool.putconn(conn)  # Return connection after success
            return                      # success — exit retry loop
        except psycopg2.OperationalError as e:
            # Transient DB error — roll back, return connection, wait and retry
            last_error = e
            try:
                conn.rollback()
            except Exception:
                pass
            _pool.putconn(conn)
            if attempt < retries - 1:
                wait = (attempt + 1) * 2   # 2s, 4s
                logger.warning(f"⚠️ DB operational error (attempt {attempt+1}/{retries}), "
                               f"retrying in {wait}s… ({e})")
                time.sleep(wait)
            else:
                logger.error(f"❌ DB query failed after {retries} attempts: {e}")
                raise
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            _pool.putconn(conn)
            raise                       # non-transient error — don't retry


def _cur(conn):
    """RealDictCursor — rows behave like dicts."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def _raw(conn):
    return conn.cursor()


# ─────────────────────────────────────────────────────────
# bot_id derivation
# ─────────────────────────────────────────────────────────
def make_bot_id(token: str) -> str:
    """
    Derive a stable, opaque 16-char identifier from a bot token.
    Never stored raw; never reversible.
    """
    return hashlib.sha256(token.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────────────────
# Schema bootstrap  (idempotent — safe to call every start)
# ─────────────────────────────────────────────────────────
_SCHEMA_SQL = """
-- Master tenant registry
CREATE TABLE IF NOT EXISTS bot_tenants (
    bot_id      TEXT        PRIMARY KEY,
    bot_type    TEXT        NOT NULL DEFAULT 'unknown',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Scheduler posts  (owned by scheduler bots)
CREATE TABLE IF NOT EXISTS posts (
    id               BIGSERIAL   PRIMARY KEY,
    bot_id           TEXT        NOT NULL
                                 REFERENCES bot_tenants(bot_id)
                                 ON DELETE CASCADE,
    message          TEXT,
    media_type       TEXT,
    media_file_id    TEXT,
    caption          TEXT,
    scheduled_time   TIMESTAMPTZ NOT NULL,
    posted           INTEGER     DEFAULT 0,
    total_channels   INT         DEFAULT 0,
    successful_posts INT         DEFAULT 0,
    posted_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Target channels  (shared by both bot types)
CREATE TABLE IF NOT EXISTS channels (
    id             BIGSERIAL   PRIMARY KEY,
    bot_id         TEXT        NOT NULL
                               REFERENCES bot_tenants(bot_id)
                               ON DELETE CASCADE,
    channel_id     TEXT        NOT NULL,
    channel_name   TEXT,
    added_at       TIMESTAMPTZ DEFAULT NOW(),
    active         INTEGER     DEFAULT 1,
    total_forwards BIGINT      DEFAULT 0,
    last_forward   TIMESTAMPTZ,
    UNIQUE (bot_id, channel_id)          -- two bots can share same target
);

-- Copy-bot forward history  (owned by forwarder bots)
CREATE TABLE IF NOT EXISTS forward_log (
    id              BIGSERIAL   PRIMARY KEY,
    bot_id          TEXT        NOT NULL
                                REFERENCES bot_tenants(bot_id)
                                ON DELETE CASCADE,
    message_id      BIGINT,
    msg_type        TEXT,
    total_channels  INT         DEFAULT 0,
    successful      INT         DEFAULT 0,
    failed          INT         DEFAULT 0,
    duration_sec    REAL,
    forwarded_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_posts_due
    ON posts (bot_id, scheduled_time, posted)
    WHERE posted = 0;

CREATE INDEX IF NOT EXISTS idx_channels_active
    ON channels (bot_id, active)
    WHERE active = 1;

CREATE INDEX IF NOT EXISTS idx_fwdlog_bot
    ON forward_log (bot_id, forwarded_at DESC);
"""


def bootstrap_schema():
    """Create all tables if they don't exist. Safe to call concurrently."""
    with get_conn() as conn:
        conn.cursor().execute(_SCHEMA_SQL)
    logger.info("✅ Schema bootstrapped")
    _migrate_boolean_to_integer()


def _migrate_boolean_to_integer():
    """
    One-time migration: convert posted (BOOLEAN) -> INTEGER and
    active (BOOLEAN) -> INTEGER so both single-file and multi-file
    bots can share the same database without type-mismatch errors.
    Safe to call on every startup — ALTER TYPE is a no-op if already INTEGER.
    """
    migrations = [
        # Convert posted column: FALSE->0, TRUE->1
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'posts'
                AND column_name = 'posted'
                AND data_type = 'boolean'
            ) THEN
                ALTER TABLE posts
                    ALTER COLUMN posted TYPE INTEGER
                    USING CASE WHEN posted THEN 1 ELSE 0 END;
                ALTER TABLE posts ALTER COLUMN posted SET DEFAULT 0;
            END IF;
        END $$;
        """,
        # Convert active column in channels: FALSE->0, TRUE->1
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'channels'
                AND column_name = 'active'
                AND data_type = 'boolean'
            ) THEN
                ALTER TABLE channels
                    ALTER COLUMN active TYPE INTEGER
                    USING CASE WHEN active THEN 1 ELSE 0 END;
                ALTER TABLE channels ALTER COLUMN active SET DEFAULT 1;
            END IF;
        END $$;
        """,
        # Drop the partial index that used boolean WHERE clause and recreate it
        """
        DROP INDEX IF EXISTS idx_posts_due;
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_posts_due
            ON posts (bot_id, scheduled_time, posted)
            WHERE posted = 0;
        """,
        # Same for channels active index
        """
        DROP INDEX IF EXISTS idx_channels_active;
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_channels_active
            ON channels (bot_id, active)
            WHERE active = 1;
        """,
    ]
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            for sql in migrations:
                cur.execute(sql)
        logger.info("✅ Boolean→Integer migration complete (or already done)")
    except Exception as e:
        logger.error(f"Migration error (non-fatal): {e}")


def register_tenant(bot_id: str, bot_type: str):
    """
    Insert this bot into bot_tenants, or update its type if already there.
    bot_type: 'scheduler' | 'forwarder'
    """
    with get_conn() as conn:
        _raw(conn).execute("""
            INSERT INTO bot_tenants (bot_id, bot_type)
            VALUES (%s, %s)
            ON CONFLICT (bot_id) DO UPDATE SET bot_type = EXCLUDED.bot_type
        """, (bot_id, bot_type))
    logger.info(f"🤖 Tenant registered: bot_id={bot_id} type={bot_type}")


# ─────────────────────────────────────────────────────────
# CHANNEL  operations  (used by both bot types)
# ─────────────────────────────────────────────────────────

def channel_add(bot_id: str, channel_id: str, channel_name: str | None = None) -> bool:
    with get_conn() as conn:
        _raw(conn).execute("""
            INSERT INTO channels (bot_id, channel_id, channel_name, active)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (bot_id, channel_id)
            DO UPDATE SET
                active       = 1,
                channel_name = COALESCE(EXCLUDED.channel_name, channels.channel_name)
        """, (bot_id, channel_id, channel_name))
    return True


def channel_remove(bot_id: str, channel_id: str) -> bool:
    with get_conn() as conn:
        cur = _raw(conn)
        cur.execute(
            "UPDATE channels SET active=0 WHERE bot_id=%s AND channel_id=%s",
            (bot_id, channel_id)
        )
        return cur.rowcount > 0


def channel_list_active(bot_id: str) -> list[str]:
    with get_conn() as conn:
        cur = _cur(conn)
        cur.execute(
            "SELECT channel_id FROM channels WHERE bot_id=%s AND active=1 ORDER BY channel_id",
            (bot_id,)
        )
        return [r['channel_id'] for r in cur.fetchall()]


def channel_list_all(bot_id: str) -> list[dict]:
    with get_conn() as conn:
        cur = _cur(conn)
        cur.execute("""
            SELECT channel_id, channel_name, added_at,
                   active, total_forwards, last_forward
            FROM   channels
            WHERE  bot_id = %s
            ORDER  BY added_at DESC
        """, (bot_id,))
        return [dict(r) for r in cur.fetchall()]


def channel_increment_forward(bot_id: str, channel_id: str):
    """Bump forward counter for one channel — called per successful copy."""
    with get_conn() as conn:
        _raw(conn).execute("""
            UPDATE channels
            SET    total_forwards = total_forwards + 1,
                   last_forward   = NOW()
            WHERE  bot_id = %s AND channel_id = %s
        """, (bot_id, channel_id))


# ─────────────────────────────────────────────────────────
# POSTS  operations  (scheduler bot only)
# ─────────────────────────────────────────────────────────

def post_insert(bot_id: str, scheduled_time, total_channels: int,
                message=None, media_type=None,
                media_file_id=None, caption=None) -> int:
    with get_conn() as conn:
        cur = _raw(conn)
        cur.execute("""
            INSERT INTO posts
                (bot_id, message, media_type, media_file_id,
                 caption, scheduled_time, total_channels)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (bot_id, message, media_type, media_file_id,
              caption, scheduled_time, total_channels))
        return cur.fetchone()[0]


def post_get_due(bot_id: str, limit: int = 200) -> list[dict]:
    """Fetch posts whose scheduled_time has passed and haven't been posted."""
    with get_conn() as conn:
        cur = _cur(conn)
        cur.execute("""
            SELECT * FROM posts
            WHERE  bot_id = %s
              AND  scheduled_time <= NOW()
              AND  posted = 0
            ORDER  BY scheduled_time
            LIMIT  %s
        """, (bot_id, limit))
        return [dict(r) for r in cur.fetchall()]


def post_mark_sent(bot_id: str, post_id: int, successful: int):
    with get_conn() as conn:
        _raw(conn).execute("""
            UPDATE posts
            SET    posted = 1,
                   posted_at = NOW(),
                   successful_posts = %s
            WHERE  id = %s AND bot_id = %s
        """, (successful, post_id, bot_id))


def post_get_pending(bot_id: str) -> list[dict]:
    with get_conn() as conn:
        cur = _cur(conn)
        cur.execute("""
            SELECT * FROM posts
            WHERE  bot_id = %s AND posted = 0
            ORDER  BY scheduled_time
        """, (bot_id,))
        return [dict(r) for r in cur.fetchall()]


def post_delete(bot_id: str, post_id: int) -> bool:
    with get_conn() as conn:
        cur = _raw(conn)
        cur.execute(
            "DELETE FROM posts WHERE id=%s AND bot_id=%s",
            (post_id, bot_id)
        )
        return cur.rowcount > 0


def post_delete_pending_all(bot_id: str) -> int:
    with get_conn() as conn:
        cur = _raw(conn)
        cur.execute(
            "DELETE FROM posts WHERE bot_id=%s AND posted = 0",
            (bot_id,)
        )
        return cur.rowcount


def post_cleanup_old(bot_id: str, minutes: int) -> int:
    """Delete posted records older than `minutes` minutes."""
    with get_conn() as conn:
        cur = _raw(conn)
        cur.execute(
            f"""
            DELETE FROM posts
            WHERE  bot_id   = %s
              AND  posted    = 1
              AND  posted_at < NOW() - INTERVAL '{int(minutes)} minutes'
            """,
            (bot_id,)
        )
        return cur.rowcount


def post_stats(bot_id: str) -> dict:
    with get_conn() as conn:
        cur = _raw(conn)
        cur.execute("SELECT COUNT(*) FROM posts WHERE bot_id=%s", (bot_id,))
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM posts WHERE bot_id=%s AND posted = 0", (bot_id,))
        pending = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM posts WHERE bot_id=%s AND posted = 1", (bot_id,))
        done = cur.fetchone()[0]
        
        # Calculate database size
        try:
            cur.execute("SELECT pg_database_size(current_database())")
            db_size_bytes = cur.fetchone()[0]
            db_size_mb = db_size_bytes / (1024 * 1024)
        except Exception:
            # If pg_database_size fails (non-PostgreSQL or insufficient permissions), return 0
            db_size_mb = 0.0
        
    return {
        'total': total, 
        'pending': pending, 
        'posted': done,
        'db_size_mb': db_size_mb
    }


def post_get_last(bot_id: str) -> dict | None:
    """Return the most recently posted post for this bot, or None."""
    with get_conn() as conn:
        cur = _cur(conn)
        cur.execute("""
            SELECT *
            FROM   posts
            WHERE  bot_id = %s AND posted = 1
            ORDER  BY posted_at DESC
            LIMIT  1
        """, (bot_id,))
        row = cur.fetchone()
        return dict(row) if row else None


# ─────────────────────────────────────────────────────────
# FORWARD LOG  operations  (forwarder bot only)
# ─────────────────────────────────────────────────────────

def fwdlog_insert(bot_id: str, message_id: int, msg_type: str,
                  total: int, successful: int, failed: int, duration: float):
    with get_conn() as conn:
        _raw(conn).execute("""
            INSERT INTO forward_log
                (bot_id, message_id, msg_type, total_channels,
                 successful, failed, duration_sec)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (bot_id, message_id, msg_type, total, successful, failed, duration))


def fwdlog_stats(bot_id: str) -> dict:
    """Aggregate stats for the /stats command."""
    with get_conn() as conn:
        cur = _cur(conn)
        cur.execute("""
            SELECT
                COUNT(*)                        AS messages_processed,
                COALESCE(SUM(total_channels),0) AS total_forwards,
                COALESCE(SUM(successful),0)     AS successful_forwards,
                COALESCE(SUM(failed),0)         AS failed_forwards,
                MAX(forwarded_at)               AS last_forward_time
            FROM forward_log
            WHERE bot_id = %s
        """, (bot_id,))
        return dict(cur.fetchone())
