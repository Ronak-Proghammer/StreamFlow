"""
StreamFlow Event Processor
===========================
Runs scheduled aggregation jobs against the events table
and writes results to event_aggregates.

This is the "Batch Layer" of our Lambda Architecture:
  - Raw events come in via RabbitMQ → Elixir consumer
  - Every 30 seconds, this processor aggregates them
  - The API reads from aggregates for fast responses

WHY A SEPARATE PYTHON SERVICE FOR THIS?
- Python has excellent data processing libraries (pandas, scipy)
- We can scale the processor independently of the API
- Keeps the Elixir app focused on real-time event handling
- In a larger system, this could be replaced by Apache Spark
  or dbt for more complex transformations
"""

import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S"
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://streamflow:streamflow_pass@localhost:5432/streamflow_dev"
)
AGGREGATION_INTERVAL_SECONDS = int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "30"))


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    """
    Create a database connection.
    
    We use psycopg2 directly here (no ORM) for maximum
    control over our analytical queries.
    
    psycopg2.extras.RealDictCursor returns rows as dicts
    instead of tuples — much easier to work with.
    """
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


# ============================================================
# AGGREGATION JOBS
# ============================================================

def run_hourly_aggregation():
    """
    Aggregate events by hour for the last 2 hours.
    
    We look back 2 hours (not 1) to handle late-arriving events.
    In distributed systems, events can arrive slightly out of order.
    
    Uses INSERT ... ON CONFLICT DO UPDATE (UPSERT) so it's
    idempotent — safe to run multiple times without duplicates.
    """
    logger.info("Running hourly aggregation...")
    
    # The SQL uses several advanced features worth knowing:
    # 1. date_trunc('hour', ...) — rounds timestamps to the hour
    # 2. COUNT(*) FILTER (WHERE ...) — conditional aggregation
    # 3. ON CONFLICT DO UPDATE — upsert (insert or update)
    aggregation_sql = """
        INSERT INTO event_aggregates (
            bucket_start, bucket_end, bucket_size,
            event_type, source_service,
            event_count, unique_users, unique_sessions, error_count,
            computed_at
        )
        SELECT
            date_trunc('hour', occurred_at) AS bucket_start,
            date_trunc('hour', occurred_at) + INTERVAL '1 hour' AS bucket_end,
            'hour' AS bucket_size,
            event_type::text::event_type,
            source_service,
            COUNT(*) AS event_count,
            COUNT(DISTINCT user_id) AS unique_users,
            COUNT(DISTINCT session_id) AS unique_sessions,
            COUNT(*) FILTER (WHERE event_type = 'error') AS error_count,
            NOW() AS computed_at
        FROM events
        WHERE occurred_at >= NOW() - INTERVAL '2 hours'
          AND occurred_at < date_trunc('hour', NOW())
        GROUP BY 1, 2, 3, 4, 5
        ON CONFLICT (bucket_start, bucket_size, event_type, source_service)
        DO UPDATE SET
            event_count     = EXCLUDED.event_count,
            unique_users    = EXCLUDED.unique_users,
            unique_sessions = EXCLUDED.unique_sessions,
            error_count     = EXCLUDED.error_count,
            computed_at     = NOW();
    """
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(aggregation_sql)
                conn.commit()
                logger.info(f"Hourly aggregation complete. Rows affected: {cur.rowcount}")
    except Exception as e:
        logger.error(f"Hourly aggregation failed: {e}")


def run_anomaly_detection():
    """
    Detect anomalies in recent event data.
    
    Simple rule-based detection for now:
    - Error rate > 10% in last 5 minutes = anomaly
    - Zero events in last 2 minutes = anomaly (could mean producer is down)
    
    In a production system you'd use statistical methods
    (z-score, moving averages, ML models).
    """
    logger.info("Running anomaly detection...")
    
    anomaly_sql = """
        WITH recent_stats AS (
            SELECT
                COUNT(*)                                          AS total,
                COUNT(*) FILTER (WHERE event_type = 'error')      AS errors,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE event_type = 'error')
                    / NULLIF(COUNT(*), 0), 2
                )                                                  AS error_rate_pct
            FROM events
            WHERE occurred_at >= NOW() - INTERVAL '5 minutes'
        )
        INSERT INTO anomalies (anomaly_type, severity, description, metadata)
        SELECT
            'high_error_rate',
            CASE
                WHEN error_rate_pct >= 25 THEN 'critical'
                WHEN error_rate_pct >= 15 THEN 'high'
                ELSE 'medium'
            END,
            format('Error rate is %.1f%% over last 5 minutes (%s errors out of %s total)',
                   error_rate_pct, errors, total),
            jsonb_build_object(
                'error_rate_pct', error_rate_pct,
                'error_count', errors,
                'total_count', total,
                'window', '5 minutes'
            )
        FROM recent_stats
        WHERE error_rate_pct > 10
          -- Don't spam duplicate anomalies — only insert if we haven't
          -- recorded this anomaly in the last 10 minutes
          AND NOT EXISTS (
              SELECT 1 FROM anomalies
              WHERE anomaly_type = 'high_error_rate'
                AND detected_at >= NOW() - INTERVAL '10 minutes'
                AND resolved = FALSE
          );
    """
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(anomaly_sql)
                if cur.rowcount > 0:
                    logger.warning(f"🚨 Anomaly detected! {cur.rowcount} anomaly record(s) inserted.")
                conn.commit()
    except Exception as e:
        logger.error(f"Anomaly detection failed: {e}")


def mark_processed_events():
    """
    Update events that have been aggregated to 'processed' status.
    Only mark events older than 1 hour (so current events stay 'received').
    """
    update_sql = """
        UPDATE events
        SET status = 'processed'
        WHERE status = 'received'
          AND occurred_at < NOW() - INTERVAL '1 hour'
          AND occurred_at >= NOW() - INTERVAL '25 hours';
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(update_sql)
                conn.commit()
                if cur.rowcount > 0:
                    logger.info(f"Marked {cur.rowcount} events as processed")
    except Exception as e:
        logger.error(f"Status update failed: {e}")


# ============================================================
# SCHEDULER SETUP
# ============================================================

def main():
    logger.info("Starting StreamFlow Processor...")
    
    # Wait for PostgreSQL to be ready
    max_retries = 10
    for attempt in range(max_retries):
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            logger.info("✅ Connected to PostgreSQL")
            break
        except Exception as e:
            wait = min(2 ** attempt, 30)
            logger.warning(f"DB not ready (attempt {attempt+1}): {e}. Retrying in {wait}s...")
            time.sleep(wait)
    else:
        logger.error("Could not connect to PostgreSQL. Exiting.")
        sys.exit(1)
    
    # APScheduler runs jobs on a schedule in the background
    scheduler = BlockingScheduler()
    
    # Aggregate events every N seconds (configurable)
    scheduler.add_job(
        run_hourly_aggregation,
        IntervalTrigger(seconds=AGGREGATION_INTERVAL_SECONDS),
        id="hourly_aggregation",
        name="Hourly event aggregation",
        max_instances=1,  # Never run two aggregations simultaneously
        coalesce=True,    # If we miss a run, just run once (don't catch up)
    )
    
    # Anomaly detection every minute
    scheduler.add_job(
        run_anomaly_detection,
        IntervalTrigger(seconds=60),
        id="anomaly_detection",
        name="Anomaly detection",
        max_instances=1,
        coalesce=True,
    )
    
    # Mark processed events every 5 minutes
    scheduler.add_job(
        mark_processed_events,
        IntervalTrigger(seconds=300),
        id="mark_processed",
        name="Mark processed events",
        max_instances=1,
    )
    
    # Run immediately on startup (don't wait for first interval)
    run_hourly_aggregation()
    run_anomaly_detection()
    
    logger.info(f"Scheduler started. Aggregating every {AGGREGATION_INTERVAL_SECONDS}s")
    
    # Handle graceful shutdown
    def shutdown(signum, frame):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown(wait=False)
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()