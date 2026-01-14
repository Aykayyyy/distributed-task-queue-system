import os, json, time
import pika
import psycopg2
import redis

# Redis
redis_host = os.getenv("REDIS_HOST", "redis")
r = redis.Redis(host=redis_host, port=6379, decode_responses=True)

# Postgres
def db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        dbname=os.getenv("DB_NAME", "dtqdb"),
        user=os.getenv("DB_USER", "dtq"),
        password=os.getenv("DB_PASSWORD", "dtqpass"),
    )

def safe_update(task_id: str, status: str, result: str = ""):
    """Update DB and refresh cache"""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE tasks SET status=%s, result=%s, updated_at=%s WHERE id=%s",
        (status, result, int(time.time()), task_id),
    )
    conn.commit()
    cur.close()
    conn.close()

    # Refresh cache too (best-effort)
    cache_key = f"task:{task_id}"
    r.setex(cache_key, 30, json.dumps({"task_id": task_id, "status": status, "result": result}))

def process(payload: str) -> str:
    """Simulated background work"""
    time.sleep(2)
    return payload.upper()

def main():
    rabbit_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    params = pika.URLParameters(rabbit_url)

    # retry loop (simple fault tolerance)
    while True:
        try:
            connection = pika.BlockingConnection(params)
            break
        except Exception:
            time.sleep(2)

    channel = connection.channel()
    channel.queue_declare(queue="task_queue", durable=True)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        task = json.loads(body.decode())
        task_id = task["id"]
        payload = task["payload"]

        safe_update(task_id, "processing", "")

        try:
            result = process(payload)
            safe_update(task_id, "done", result)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            # If something fails, mark and requeue
            safe_update(task_id, "failed", "processing error")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue="task_queue", on_message_callback=callback)
    channel.start_consuming()

if __name__ == "__main__":
    main()
