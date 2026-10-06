"""One-shot setup, run by the `bootstrap` compose service before anything else starts.

1. PostgreSQL schema (a real project would use Alembic migrations; create_all keeps the focus on Celery).
2. RabbitMQ topology. This matters more than it looks: dead-lettering publishes to the `dlx` exchange,
   and if that exchange (or the dead_letter queue bound to it) does not exist, RabbitMQ silently DROPS
   the message. No worker consumes `dead_letter`, so nothing else would ever declare it.

The AMQP calls go through kombu, the messaging library Celery is built on, so there is no separate
client (e.g. pika) to install or learn.
"""
from app.celery_app import celery_app
from config import DEAD_LETTER_EXCHANGE, TASK_QUEUES
from database import init_db


def declare_broker_topology() -> None:
    with celery_app.connection_for_write() as conn:
        channel = conn.default_channel
        DEAD_LETTER_EXCHANGE(channel).declare()
        for queue in TASK_QUEUES:  # also declares each queue's exchange + binding; idempotent
            queue(channel).declare()


if __name__ == "__main__":
    init_db()
    declare_broker_topology()
    print("schema + broker topology ready")
