"""
StreamFlow Event Producer
=========================
Generates realistic fake events and publishes them to RabbitMQ.
"""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import pika
from faker import Faker

fake = Faker()

RABBITMQ_HOST  = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT  = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER  = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS  = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "2"))

# ─── Topology constants ────────────────────────────────────────────────────────
# These MUST match exactly what the Elixir consumer expects
# Both sides declare the same topology — safe because RabbitMQ
# declare operations are idempotent (declaring twice = no error)
EXCHANGE       = "streamflow.events"
QUEUE          = "streamflow.events.queue"
DEAD_LETTER_Q  = "streamflow.events.dead_letter"
DEAD_LETTER_EX = "streamflow.events.dead_letter_exchange"
ROUTING_KEY    = "events"

SERVICES = [
  "web-frontend", "mobile-app", "auth-service",
  "payment-service", "api-gateway", "recommendation-service",
]

EVENT_TYPES_WEIGHTED = [
  ("page_view",    35),
  ("button_click", 25),
  ("api_call",     20),
  ("user_login",    8),
  ("user_logout",   5),
  ("purchase",      4),
  ("error",         2),
  ("data_export",   1),
]
EVENT_TYPES   = [e[0] for e in EVENT_TYPES_WEIGHTED]
EVENT_WEIGHTS = [e[1] for e in EVENT_TYPES_WEIGHTED]


def generate_event(event_type: str, service: str) -> dict:
  base = {
    "id":             str(uuid.uuid4()),
    "source_service": service,
    "event_type":     event_type,
    "user_id":        f"user_{random.randint(1, 200):04d}" if random.random() > 0.1 else None,
    "session_id":     f"sess_{uuid.uuid4().hex[:16]}",
    "occurred_at":    datetime.now(timezone.utc).isoformat(),
    "client_ip":      fake.ipv4(),
  }

  if event_type == "page_view":
    pages = ["/", "/dashboard", "/settings", "/profile", "/reports", "/checkout"]
    base["event_data"] = {"page": random.choice(pages), "time_on_page_ms": random.randint(100, 60000)}
  elif event_type == "button_click":
    base["event_data"] = {"button": random.choice(["submit", "cancel", "export", "delete", "save"])}
  elif event_type == "user_login":
    base["event_data"] = {"method": random.choice(["password", "oauth_google", "sso"]), "success": random.random() > 0.05}
  elif event_type == "purchase":
    base["event_data"] = {"amount": round(random.uniform(9.99, 999.99), 2), "currency": random.choice(["USD", "EUR", "GBP"])}
  elif event_type == "error":
    codes = ["AUTH_TIMEOUT", "DB_CONNECTION_FAILED", "RATE_LIMITED", "INTERNAL_SERVER_ERROR"]
    base["event_data"] = {"code": random.choice(codes), "severity": random.choice(["warning", "error", "critical"])}
  elif event_type == "api_call":
    base["event_data"] = {"endpoint": random.choice(["/v1/events", "/v1/users", "/v1/reports"]),
                          "method": random.choice(["GET", "POST"]),
                          "status_code": random.choice([200, 200, 200, 400, 500]),
                          "latency_ms": random.randint(5, 2000)}
  else:
    base["event_data"] = {}

  return base


def create_connection() -> pika.BlockingConnection:
  credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
  parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST, port=RABBITMQ_PORT,
    virtual_host=RABBITMQ_VHOST, credentials=credentials,
    heartbeat=60, blocked_connection_timeout=300,
  )
  return pika.BlockingConnection(parameters)


class EventProducer:
  def __init__(self):
    self.connection: Optional[pika.BlockingConnection] = None
    self.channel = None
    self.running = False
    self.events_sent = 0
    self.errors = 0
    signal.signal(signal.SIGTERM, self._handle_shutdown)
    signal.signal(signal.SIGINT, self._handle_shutdown)

  def _handle_shutdown(self, signum, frame):
    print(f"\n🛑 Shutting down gracefully...")
    self.running = False

  def _declare_topology(self):
    """
    Declare the full RabbitMQ topology:
      dead letter exchange → dead letter queue
      main exchange → main queue (with dead letter config)
      binding: main exchange + routing key → main queue

    WHY the producer does this too:
      The Elixir consumer also declares this topology.
      RabbitMQ declare operations are IDEMPOTENT — declaring
      something that already exists with the same args is a no-op.
      This means whoever starts first sets up the topology,
      and the second one just confirms it's already there.

      Without this, if the producer starts before Elixir,
      messages publish to the exchange but have no queue to
      route to → "Message unroutable" warning → messages dropped.
    """

    # Step 1: Dead letter exchange
    # Failed/rejected messages go here instead of being lost
    self.channel.exchange_declare(
      exchange=DEAD_LETTER_EX,
      exchange_type="direct",
      durable=True    # survives RabbitMQ restart
    )

    # Step 2: Dead letter queue
    self.channel.queue_declare(
      queue=DEAD_LETTER_Q,
      durable=True
    )

    # Step 3: Bind dead letter queue to dead letter exchange
    self.channel.queue_bind(
      queue=DEAD_LETTER_Q,
      exchange=DEAD_LETTER_EX,
      routing_key=ROUTING_KEY
    )

    # Step 4: Main exchange — where producer publishes to
    self.channel.exchange_declare(
      exchange=EXCHANGE,
      exchange_type="direct",
      durable=True
    )

    # Step 5: Main queue with dead letter routing
    # x-dead-letter-exchange: where NACKed messages go
    # x-message-ttl: auto-expire messages after 24h if unprocessed
    self.channel.queue_declare(
      queue=QUEUE,
      durable=True,
      arguments={
        "x-dead-letter-exchange":    DEAD_LETTER_EX,
        "x-dead-letter-routing-key": ROUTING_KEY,
        "x-message-ttl":             86400000,  # 24 hours in milliseconds
      }
    )

    # Step 6: THE CRITICAL BINDING — this is what was missing
    # Without this, exchange receives messages but has
    # no rule for where to send them → unroutable
    #
    # This binding says:
    # "messages arriving at EXCHANGE with ROUTING_KEY → go to QUEUE"
    self.channel.queue_bind(
      queue=QUEUE,
      exchange=EXCHANGE,
      routing_key=ROUTING_KEY
    )

    print("✅ Topology declared (exchange → binding → queue)")

  def connect(self):
    for attempt in range(10):
      try:
        print(f"🔌 Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT} (vhost: {RABBITMQ_VHOST})...")
        self.connection = create_connection()
        self.channel = self.connection.channel()

        # Declare full topology before publishing anything
        self._declare_topology()

        # Confirm delivery — if a message is unroutable,
        # pika raises an exception instead of silently dropping it
        self.channel.confirm_delivery()

        print("✅ Connected!")
        return

      except Exception as e:
        wait = min(2 ** attempt, 30)
        print(f"❌ Failed (attempt {attempt+1}): {e}. Retrying in {wait}s...")
        time.sleep(wait)
    sys.exit(1)

  def publish(self, event: dict) -> bool:
    try:
      self.channel.basic_publish(
        exchange=EXCHANGE, routing_key=ROUTING_KEY,
        body=json.dumps(event, default=str),
        properties=pika.BasicProperties(
          content_type="application/json",
          delivery_mode=2,           # persistent — survives RabbitMQ restart
          message_id=event["id"],
        ),
        mandatory=True,              # raise error if unroutable (don't silently drop)
      )
      return True
    except pika.exceptions.UnroutableError:
      # This should no longer happen now that topology is declared
      # But handle it explicitly so we know if something is wrong
      print(f"⚠️  Message unroutable — check exchange/queue binding")
      return False
    except Exception as e:
      print(f"❌ Publish failed: {e}")
      return False

  def run(self):
    self.connect()
    self.running = True
    interval = 1.0 / EVENTS_PER_SECOND
    print(f"🚀 Producing {EVENTS_PER_SECOND} events/second...")

    while self.running:
      try:
        service    = random.choice(SERVICES)
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        event      = generate_event(event_type, service)

        if self.publish(event):
          self.events_sent += 1
          if self.events_sent % 100 == 0:
            print(f"📊 Sent {self.events_sent} | Errors: {self.errors}")
        else:
          self.errors += 1

        time.sleep(interval)

      except pika.exceptions.AMQPConnectionError:
        print("🔄 Reconnecting...")
        self.connect()
      except Exception as e:
        print(f"❌ Error: {e}")
        self.errors += 1
        time.sleep(1)

    print(f"📈 Done. Sent: {self.events_sent}, Errors: {self.errors}")
    if self.connection and not self.connection.is_closed:
      self.connection.close()


if __name__ == "__main__":
  EventProducer().run()