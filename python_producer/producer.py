"""
StreamFlow Event Producer
=========================
Generates realistic fake events and publishes them to RabbitMQ.

In a real system, this would be replaced by actual application
services (web frontend, mobile apps, etc.) that publish real
user events. For our demo, we simulate that traffic.

DESIGN PATTERN: Publisher/Producer
- Does NOT know who will consume the events
- Does NOT know what will happen to the events
- Just puts them in the queue and moves on
- This decoupling is the core value of message queues
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

# ============================================================
# CONFIGURATION
# ============================================================
# We read config from environment variables — never hardcode
# credentials in source code. In production this would come
# from a secrets manager (AWS Secrets Manager, Vault, etc.)
# ============================================================
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "2"))

EXCHANGE = "streamflow.events"
ROUTING_KEY = "events"

# ============================================================
# EVENT GENERATORS
# ============================================================
# We use a list of services and weighted event types to
# simulate realistic traffic patterns.
# Purchases are rare; page views are common.
# This mimics real-world traffic distributions.
# ============================================================

SERVICES = [
    "web-frontend",
    "mobile-app",
    "auth-service",
    "payment-service",
    "api-gateway",
    "recommendation-service",
]

# Weighted choices — page_view happens most often
EVENT_TYPES_WEIGHTED = [
    ("page_view", 35),
    ("button_click", 25),
    ("api_call", 20),
    ("user_login", 8),
    ("user_logout", 5),
    ("purchase", 4),
    ("error", 2),
    ("data_export", 1),
]

# Unpack for random.choices
EVENT_TYPES = [e[0] for e in EVENT_TYPES_WEIGHTED]
EVENT_WEIGHTS = [e[1] for e in EVENT_TYPES_WEIGHTED]


def generate_event_data(event_type: str, service: str) -> dict:
    """
    Generate realistic payload data for each event type.
    
    In a real system this data would come from the actual application.
    Each event type has a different shape — this is why we use JSONB
    in PostgreSQL (flexible schema).
    """
    base = {
        "id": str(uuid.uuid4()),
        "source_service": service,
        "event_type": event_type,
        "user_id": f"user_{random.randint(1, 200):04d}" if random.random() > 0.1 else None,
        "session_id": f"sess_{uuid.uuid4().hex[:16]}",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "client_ip": fake.ipv4(),
    }

    # Event-specific payload
    if event_type == "page_view":
        pages = ["/", "/dashboard", "/settings", "/profile", "/reports", "/checkout", "/login"]
        base["event_data"] = {
            "page": random.choice(pages),
            "referrer": random.choice(pages + [None]),
            "time_on_page_ms": random.randint(100, 60000),
            "viewport": {"width": 1920, "height": 1080},
        }

    elif event_type == "button_click":
        base["event_data"] = {
            "button_id": random.choice(["submit", "cancel", "export", "delete", "save", "next"]),
            "page": "/dashboard",
            "x_pos": random.randint(0, 1920),
            "y_pos": random.randint(0, 1080),
        }

    elif event_type == "user_login":
        base["event_data"] = {
            "method": random.choice(["password", "oauth_google", "oauth_github", "sso"]),
            "success": random.random() > 0.05,  # 95% success rate
            "mfa_used": random.random() > 0.5,
        }

    elif event_type == "purchase":
        base["event_data"] = {
            "amount": round(random.uniform(9.99, 999.99), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "CAD"]),
            "product_id": f"prod_{random.randint(1, 100):03d}",
            "payment_method": random.choice(["credit_card", "paypal", "apple_pay"]),
        }

    elif event_type == "error":
        error_codes = ["AUTH_TIMEOUT", "DB_CONNECTION_FAILED", "RATE_LIMITED",
                      "INVALID_INPUT", "INTERNAL_SERVER_ERROR", "PAYMENT_DECLINED"]
        base["event_data"] = {
            "code": random.choice(error_codes),
            "message": fake.sentence(),
            "stack_trace": f"Error at {service}:{random.randint(10, 500)}",
            "severity": random.choice(["warning", "error", "critical"]),
        }

    elif event_type == "api_call":
        endpoints = ["/v1/events", "/v1/users", "/v1/reports", "/v1/export", "/v1/stats"]
        methods = ["GET", "POST", "PUT", "DELETE"]
        base["event_data"] = {
            "endpoint": random.choice(endpoints),
            "method": random.choice(methods),
            "status_code": random.choice([200, 200, 200, 201, 400, 401, 404, 500]),
            "latency_ms": random.randint(5, 2000),
        }

    else:
        base["event_data"] = {}

    return base


# ============================================================
# RABBITMQ CONNECTION
# ============================================================

def create_connection() -> pika.BlockingConnection:
    """
    Create a RabbitMQ connection with retry logic.
    
    pika.BlockingConnection is simple but blocks the thread.
    For production with high throughput, you'd use
    pika.SelectConnection (async) or a different library.
    
    We use connection parameters with heartbeat to detect
    dead connections quickly.
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        # Heartbeat: how often to ping RabbitMQ to detect dead connections
        # 60 seconds is a good balance between sensitivity and overhead
        heartbeat=60,
        # How long to wait for a connection (seconds)
        blocked_connection_timeout=300,
    )
    return pika.BlockingConnection(parameters)


def setup_channel(connection: pika.BlockingConnection) -> pika.adapters.blocking_connection.BlockingChannel:
    """Set up channel with exchange declaration."""
    channel = connection.channel()
    
    # Declare exchange (idempotent — safe to call multiple times)
    # durable=True: exchange survives RabbitMQ restart
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    
    # Publisher confirms: RabbitMQ sends an ACK when it receives our message
    # Without this, we're sending fire-and-forget with no guarantee
    # of delivery. In production, ALWAYS use publisher confirms.
    channel.confirm_delivery()
    
    return channel


# ============================================================
# MAIN PRODUCER LOOP
# ============================================================

class EventProducer:
    def __init__(self):
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel = None
        self.running = False
        self.events_sent = 0
        self.errors = 0
        
        # Handle graceful shutdown on SIGTERM/SIGINT
        # In Docker/Kubernetes, SIGTERM is sent before SIGKILL
        # giving us time to finish processing and close connections
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        print(f"\n🛑 Received signal {signum}. Shutting down gracefully...")
        self.running = False

    def connect(self):
        """Connect with exponential backoff retry."""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                print(f"🔌 Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
                self.connection = create_connection()
                self.channel = setup_channel(self.connection)
                print("✅ Connected to RabbitMQ!")
                return
            except Exception as e:
                wait = min(2 ** attempt, 30)  # Exponential backoff, max 30s
                print(f"❌ Connection failed (attempt {attempt+1}/{max_retries}): {e}")
                print(f"⏳ Retrying in {wait}s...")
                time.sleep(wait)
        
        print("💥 Could not connect to RabbitMQ after max retries. Exiting.")
        sys.exit(1)

    def publish_event(self, event: dict) -> bool:
        """
        Publish a single event to RabbitMQ.
        
        We serialize to JSON and set content_type so consumers
        know how to deserialize. persistent=2 means the message
        is written to disk (survives broker restart).
        """
        try:
            payload = json.dumps(event, default=str)
            
            self.channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=ROUTING_KEY,
                body=payload,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,  # Persistent (survives restart)
                    message_id=event["id"],
                    timestamp=int(time.time()),
                ),
                mandatory=True,  # Raise error if no queue is bound
            )
            return True
        
        except pika.exceptions.UnroutableError:
            print(f"⚠️  Message unroutable — is a queue bound to the exchange?")
            return False
        except Exception as e:
            print(f"❌ Publish failed: {e}")
            return False

    def run(self):
        """Main production loop."""
        self.connect()
        self.running = True
        
        interval = 1.0 / EVENTS_PER_SECOND
        
        print(f"🚀 Starting event production at {EVENTS_PER_SECOND} events/second")
        print(f"   Exchange: {EXCHANGE}")
        print(f"   Press Ctrl+C to stop\n")
        
        while self.running:
            try:
                # Pick a random service and event type
                service = random.choice(SERVICES)
                event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
                
                # Generate the event
                event = generate_event_data(event_type, service)
                
                # Publish it
                if self.publish_event(event):
                    self.events_sent += 1
                    if self.events_sent % 100 == 0:
                        print(f"📊 Sent {self.events_sent} events | Errors: {self.errors}")
                else:
                    self.errors += 1
                
                # Sleep to maintain our target rate
                time.sleep(interval)
                
            except pika.exceptions.AMQPConnectionError:
                print("🔄 Connection lost. Reconnecting...")
                self.connect()
            
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                self.errors += 1
                time.sleep(1)
        
        # Graceful shutdown
        print(f"\n📈 Final stats: {self.events_sent} events sent, {self.errors} errors")
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("🔌 RabbitMQ connection closed.")


if __name__ == "__main__":
    producer = EventProducer()
    producer.run()