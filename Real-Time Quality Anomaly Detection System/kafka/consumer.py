import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import yaml
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from kafka import KafkaConsumer, KafkaProducer

DLQ_TOPIC = "serum.source.qa_events.dlq"

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "configs" / "config.yaml"
LOG_PATH = BASE_DIR / "logs" / "consumer.log"
LOG_PATH.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH)],
)
log = logging.getLogger(__name__)


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def get_dlq_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        value_serializer=lambda m: json.dumps(m, default=str).encode("utf-8"),
    )


def publish_to_dlq(
    producer: KafkaProducer,
    raw_message,
    error: Exception,
    error_type: str,
    partition: int,
    offset: int,
) -> None:
    dlq_payload = {
        "failed_at": datetime.utcnow().isoformat(),
        "error_type": error_type,
        "error": str(error),
        "kafka_partition": partition,
        "kafka_offset": offset,
        "raw_message": raw_message,
    }
    producer.send(DLQ_TOPIC, dlq_payload)
    producer.flush()
    log.warning(
        f"Published failed message to DLQ — "
        f"partition={partition}, offset={offset}, error={error_type}: {error}"
    )


def get_consumer(config: dict) -> KafkaConsumer:
    return KafkaConsumer(
        os.getenv("KAFKA_TOPIC", "serum.source.qa_events"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        group_id=config["kafka"]["consumer_group"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


def parse_message(msg: dict) -> tuple[dict | None, Exception | None]:
    try:
        payload = msg.get("payload", msg)
        after = payload.get("after")
        if after is None:
            return None, None  # Debezium metadata message — skip silently

        for ts_col in ["timestamp", "closed_timestamp"]:
            val = after.get(ts_col)
            if val is not None:
                after[ts_col] = datetime.fromtimestamp(val / 1_000_000, tz=timezone.utc).replace(
                    tzinfo=None
                )
        return after, None

    except Exception as e:
        log.warning(f"Failed to parse message: {e}")
        return None, e


INSERT_SQL = """
      INSERT INTO bronze.qa_events (
          source_id, timestamp, event_type, event_id, product, system,
          root_cause, severity, batch_id, status, resolution_days,
          closed_timestamp, is_anomaly, is_covid_period, hour,
          day_of_week, month, year, is_weekend
      ) VALUES %s
  """


def insert_batch(batch: list[dict], conn) -> None:
    rows = [
        (
            row.get("id"),
            row.get("timestamp"),
            row.get("event_type"),
            row.get("event_id"),
            row.get("product"),
            row.get("system"),
            row.get("root_cause"),
            row.get("severity"),
            row.get("batch_id"),
            row.get("status"),
            row.get("resolution_days"),
            row.get("closed_timestamp"),
            row.get("is_anomaly"),
            row.get("is_covid_period"),
            row.get("hour"),
            row.get("day_of_week"),
            row.get("month"),
            row.get("year"),
            row.get("is_weekend"),
        )
        for row in batch
    ]
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
    conn.commit()


def main():
    config = load_config()

    log.info("Connecting to PostgreSQL...")
    conn = get_db_connection()

    log.info("Starting Kafka consumer...")
    consumer = get_consumer(config)

    log.info("Starting DLQ producer...")
    dlq_producer = get_dlq_producer()

    try:
        for message in consumer:
            partition = message.partition
            offset = message.offset

            # ── Parse ────────────────────────────────────────────────────────
            row, parse_err = parse_message(message.value)

            if parse_err is not None:
                publish_to_dlq(
                    dlq_producer, message.value, parse_err, "ParseError", partition, offset
                )
                continue

            if row is None:
                continue  # Debezium metadata — skip silently

            # ── Insert ───────────────────────────────────────────────────────
            try:
                insert_batch([row], conn)
            except Exception as insert_err:
                log.error(f"Insert failed for offset={offset}: {insert_err}")
                conn.rollback()
                publish_to_dlq(
                    dlq_producer, message.value, insert_err, "InsertError", partition, offset
                )

    except KeyboardInterrupt:
        log.info("Shutting down consumer...")

    finally:
        consumer.close()
        dlq_producer.close()
        conn.close()
        log.info("Consumer closed.")


if __name__ == "__main__":
    main()
