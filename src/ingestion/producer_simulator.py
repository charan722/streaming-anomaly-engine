import json
import random
import time
import uuid
from typing import Generator
from kafka import KafkaProducer
from schemas import TransactionEvent

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC_NAME = "financial_transactions"

# Normal behavioral profiles
MERCHANT_CATEGORIES = ["grocery", "retail", "entertainment", "travel", "digital_goods", "utilities"]
COUNTRIES = ["US", "CA", "GB", "DE", "FR"]
FRAUD_COUNTRIES = ["RU", "NG", "RO", "UA"]

ACCOUNTS = [f"acc_{i:04d}" for i in range(100)]


def generate_event(anomaly_type: str = "none") -> TransactionEvent:
    account_id = random.choice(ACCOUNTS)
    now = time.time()
    tx_id = str(uuid.uuid4())

    if anomaly_type == "velocity_burst":
        # Card testing attack: Rapid tiny amounts
        return TransactionEvent(
            transaction_id=tx_id,
            account_id=account_id,
            amount=round(random.uniform(0.50, 3.00), 2),
            timestamp=now,
            merchant_id=f"merch_{random.randint(100, 110)}",
            merchant_category="digital_goods",
            country="US",
            is_anomaly=True,
        )

    elif anomaly_type == "amount_outlier":
        # Outlier spike: Sudden high value transaction
        return TransactionEvent(
            transaction_id=tx_id,
            account_id=account_id,
            amount=round(random.uniform(4500.00, 12000.00), 2),
            timestamp=now,
            merchant_id=f"merch_{random.randint(500, 600)}",
            merchant_category="luxury_goods",
            country="US",
            is_anomaly=True,
        )

    elif anomaly_type == "geo_jump":
        # Geographic leap
        return TransactionEvent(
            transaction_id=tx_id,
            account_id=account_id,
            amount=round(random.uniform(50.0, 350.0), 2),
            timestamp=now,
            merchant_id=f"merch_{random.randint(800, 900)}",
            merchant_category="travel",
            country=random.choice(FRAUD_COUNTRIES),
            is_anomaly=True,
        )

    # Standard benign transaction (log-normal distribution)
    amount = round(float(random.lognormvariate(mu=3.2, sigma=0.8)), 2)
    amount = max(1.0, min(amount, 800.0))  # Bound normal spending

    return TransactionEvent(
        transaction_id=tx_id,
        account_id=account_id,
        amount=amount,
        timestamp=now,
        merchant_id=f"merch_{random.randint(1, 200)}",
        merchant_category=random.choice(MERCHANT_CATEGORIES),
        country=random.choice(COUNTRIES),
        is_anomaly=False,
    )


def stream_generator() -> Generator[TransactionEvent, None, None]:
    """Yields continuous stream with ~1% anomaly injection."""
    while True:
        r = random.random()
        if r < 0.004:
            # Velocity burst injection: send 5 consecutive events in rapid succession
            burst_acc = random.choice(ACCOUNTS)
            for _ in range(5):
                evt = generate_event(anomaly_type="velocity_burst")
                evt.account_id = burst_acc
                yield evt
        elif r < 0.008:
            yield generate_event(anomaly_type="amount_outlier")
        elif r < 0.010:
            yield generate_event(anomaly_type="geo_jump")
        else:
            yield generate_event(anomaly_type="none")

        # Ingestion sleep: Poisson process simulation (~20-50 events/sec)
        time.sleep(random.expovariate(30.0))


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.to_json_bytes(),
        acks=1,  # Fast durable acknowledgment
        linger_ms=5,  # Micro-batching buffer
    )

    print(f"[*] Producer connected to {BOOTSTRAP_SERVERS}. Emitting to topic: {TOPIC_NAME}...")
    try:
        count = 0
        for event in stream_generator():
            # Partitioning strictly by account_id guarantees chronological order per entity
            producer.send(
                TOPIC_NAME,
                key=event.account_id,
                value=event,
            )
            count += 1
            if count % 100 == 0:
                print(f"[+] Dispatched {count} events | Sample -> Acc: {event.account_id} | ${event.amount:.2f} | Anomaly: {event.is_anomaly}")
    except KeyboardInterrupt:
        print("\nStopping event producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()