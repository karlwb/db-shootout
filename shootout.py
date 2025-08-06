import time
import uuid
import random
import psycopg2
from psycopg2.extras import Json, execute_values
from pymongo import MongoClient
from pymongo.operations import UpdateOne
from rich.console import Console
from rich.table import Table

# --- Configuration ---
PG_CONN_STR = "postgresql://user:password@localhost:5432/shootout_db"
MONGO_CONN_STR = "mongodb://user:password@localhost:27017/"

NUM_DOCS = 50_000
NUM_UPSERTS = 10_000
BATCH_SIZE = 1_000    

# --- Data Generation ---
def generate_simple_metric(doc_id):
    return {
        "id": str(doc_id),
        "timestamp": time.time(),
        "metadata": {"source": "sensor_A", "region": "us-east-1"},
        "values": {"temp": round(random.uniform(20.0, 30.0), 2), "humidity": random.randint(40, 60)},
    }

def generate_complex_blob(doc_id):
    return {
        "id": str(doc_id),
        "run_id": str(uuid.uuid4()),
        "status": random.choice(["SUCCESS", "FAILED", "RUNNING"]),
        "config": {"param1": "value1", "param2": random.randint(1, 100)},
        "results": [{"name": f"metric_{i}", "value": random.random() * 1000} for i in range(5)],
        "logs": "Log entry" * random.randint(1, 3),
    }

# --- Database Setup and Teardown ---
def setup_postgres(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS metrics, results;")
        cur.execute("""
            CREATE TABLE metrics (
                id UUID PRIMARY KEY,
                data JSONB
            );
        """)
        # Create a GIN index on the JSONB column for faster lookups
        cur.execute("CREATE INDEX idx_gin_metrics_data ON metrics USING GIN (data);")
        cur.execute("""
            CREATE TABLE results (
                id UUID PRIMARY KEY,
                data JSONB
            );
        """)
        conn.commit()

def setup_mongo(client):
    db = client.shootout_db
    db.drop_collection("metrics")
    db.drop_collection("results")
    db.metrics.create_index("metadata.source")
    return db

# --- Benchmark Functions ---
def benchmark(func, *args, **kwargs):
    """A simple decorator to time functions."""
    start_time = time.perf_counter()
    func(*args, **kwargs)
    end_time = time.perf_counter()
    return end_time - start_time

def pg_bulk_insert(conn, docs):
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO metrics (id, data) VALUES %s",
            [(str(doc["id"]), Json(doc)) for doc in docs],
            page_size=BATCH_SIZE
        )
    conn.commit()

def mongo_bulk_insert(collection, docs):
    collection.insert_many(docs, ordered=False)

def pg_upsert(conn, docs):
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO metrics (id, data) VALUES %s
            ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;
            """,
            [(str(doc["id"]), Json(doc)) for doc in docs],
            page_size=BATCH_SIZE
        )
    conn.commit()

def mongo_upsert(collection, docs):
    requests = [UpdateOne({"_id": doc["id"]}, {"$set": doc}, upsert=True) for doc in docs]
    collection.bulk_write(requests, ordered=False)

def pg_read_query(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT data FROM metrics WHERE data->'metadata'->>'source' = 'sensor_B';")
        _ = cur.fetchall() # Consume the results

def mongo_read_query(collection):
    _ = list(collection.find({"metadata.source": "sensor_B"}))

def pg_flexible_insert(conn, docs):
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO results (id, data) VALUES %s",
            [(str(doc["id"]), Json(doc)) for doc in docs]
        )
    conn.commit()

def mongo_flexible_insert(collection, docs):
    collection.insert_many(docs, ordered=False)


# --- Main Execution ---
def main():
    console = Console()
    console.print("[bold]Starting PostgreSQL vs. MongoDB Shootout...[/bold]")
    console.print("[bold]Configuration:[/bold]")
    console.print(f"Number of documents: {NUM_DOCS}")
    console.print(f"Number of upserts: {NUM_UPSERTS}")
    console.print(f"Batch size: {BATCH_SIZE}")

    table = Table(title=f"PostgreSQL vs. MongoDB Shootout ({NUM_DOCS} docs, {NUM_UPSERTS} upserts, batch size {BATCH_SIZE})")
    table.add_column("Test Case", style="cyan")
    table.add_column("PostgreSQL (JSONB)", style="magenta", justify="right")
    table.add_column("MongoDB (BSON)", style="green", justify="right")

    # --- Connections ---
    console.print(f"[bold]Connecting to postgres {PG_CONN_STR}...[/bold]")
    pg_conn = psycopg2.connect(PG_CONN_STR, connect_timeout=3)

    console.print(f"[bold]Connecting to MongoDB {MONGO_CONN_STR}...[/bold]")
    mongo_client = MongoClient(MONGO_CONN_STR)
    
    # --- Setup ---
    console.print("[bold]Setting up databases...[/bold]")
    setup_postgres(pg_conn)
    mongo_db = setup_mongo(mongo_client)
    pg_metrics_table, pg_results_table = "metrics", "results"
    mongo_metrics_coll = mongo_db.metrics
    mongo_results_coll = mongo_db.results

    # --- Test 1: Bulk Insert Simple Time Series ---
    console.print(f"[bold]Running Test 1: Bulk inserting {NUM_DOCS} simple metrics...[/bold]")
    simple_docs = [generate_simple_metric(uuid.uuid4()) for _ in range(NUM_DOCS)]
    
    # Mongo requires _id field for upsert matching
    mongo_simple_docs = [dict(d, _id=d['id']) for d in simple_docs]

    pg_insert_time = benchmark(pg_bulk_insert, pg_conn, simple_docs)
    mongo_insert_time = benchmark(mongo_bulk_insert, mongo_metrics_coll, mongo_simple_docs)
    table.add_row("Insert (Simple)", f"{pg_insert_time:.4f}s", f"{mongo_insert_time:.4f}s")

    # --- Test 2: Upserting Metrics ---
    console.print(f"[bold]Running Test 2: Upserting {NUM_UPSERTS} updated metrics...[/bold]")
    ids_to_update = [doc['id'] for doc in random.sample(simple_docs, NUM_UPSERTS)]
    docs_to_upsert = []
    for doc_id in ids_to_update:
        upsert_doc = generate_simple_metric(doc_id)
        upsert_doc["metadata"]["source"] = "sensor_B" # The update
        upsert_doc["updated"] = True
        docs_to_upsert.append(upsert_doc)
        
    mongo_docs_to_upsert = [dict(d, _id=d['id']) for d in docs_to_upsert]

    pg_upsert_time = benchmark(pg_upsert, pg_conn, docs_to_upsert)
    mongo_upsert_time = benchmark(mongo_upsert, mongo_metrics_coll, mongo_docs_to_upsert)
    table.add_row("Upsert", f"{pg_upsert_time:.4f}s", f"{mongo_upsert_time:.4f}s")

    # --- Test 3: Read Query ---
    console.print(f"[bold]Running Test 3: Reading all updated metrics...[/bold]")
    pg_read_time = benchmark(pg_read_query, pg_conn)
    mongo_read_time = benchmark(mongo_read_query, mongo_metrics_coll)
    table.add_row("Read Query (Indexed)", f"{pg_read_time:.4f}s", f"{mongo_read_time:.4f}s")

    # --- Test 4: Flexible Schema Insert ---
    console.print(f"[bold]Running Test 4: Bulk inserting {NUM_DOCS} varied JSON blobs...[/bold]")
    complex_docs = [generate_complex_blob(uuid.uuid4()) for _ in range(NUM_DOCS)]
    mongo_complex_docs = [dict(d, _id=d['id']) for d in complex_docs]

    pg_flex_time = benchmark(pg_flexible_insert, pg_conn, complex_docs)
    mongo_flex_time = benchmark(mongo_flexible_insert, mongo_results_coll, mongo_complex_docs)
    table.add_row("Insert (Complex/Varied)", f"{pg_flex_time:.4f}s", f"{mongo_flex_time:.4f}s")
    
    console.print(table)

    # --- Cleanup ---
    pg_conn.close()
    mongo_client.close()

if __name__ == "__main__":
    main()