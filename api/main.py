import os, json, uuid, time
from fastapi import FastAPI
import pika
import psycopg2
import redis

app = FastAPI(title="Distributed Task Queue API")

# Redis (cache)
redis_host = os.getenv("REDIS_HOST", "redis")
r = redis.Redis(host=redis_host, port=6379, decode_responses=True)

# Postgres (database)
def db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        dbname=os.getenv("DB_NAME", "dtqdb"),
        user=os.getenv("DB_USER", "dtq"),
        password=os.getenv("DB_PASSWORD", "dtqpass"),
    )

def init_db():
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
      id TEXT PRIMARY KEY,
      payload TEXT NOT NULL,
      status TEXT NOT NULL,
      result TEXT,
      created_at BIGINT NOT NULL,
      updated_at BIGINT NOT NULL
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

@app.on_event("startup")
def startup():
    init_db()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/tasks")
def create_task(payload: str):
    task_id = str(uuid.uuid4())
    now = int(time.time())

    # Save initial record to DB
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tasks(id,payload,status,result,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s)",
        (task_id, payload, "queued", "", now, now),
    )
    conn.commit()
    cur.close()
    conn.close()

    # Publish task to RabbitMQ
    rabbit_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    params = pika.URLParameters(rabbit_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue="task_queue", durable=True)

    channel.basic_publish(
        exchange="",
        routing_key="task_queue",
        body=json.dumps({"id": task_id, "payload": payload}),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    connection.close()

    return {"task_id": task_id, "status": "queued"}

@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    # First check cache
    cache_key = f"task:{task_id}"
    cached = r.get(cache_key)
    if cached:
        return json.loads(cached)

    # If not in cache, read from DB
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, status, result FROM tasks WHERE id=%s", (task_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return {"error": "Task not found"}

    data = {"task_id": row[0], "status": row[1], "result": row[2] or ""}

    # Save to cache for 30 seconds
    r.setex(cache_key, 30, json.dumps(data))
    return data
