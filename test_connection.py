from db import get_connection

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        print(cur.fetchone()[0])
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';")
        print("TimescaleDB:", cur.fetchone()[0])