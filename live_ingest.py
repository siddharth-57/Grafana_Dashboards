"""Step 6b: live mode.
Fills the gap since the last stored point, then inserts new data every 2 minutes.
Stop with Ctrl+C; run again to resume."""
import random
import time
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np

import alerting as al
import generator as g
from db import get_connection

rng = np.random.default_rng()

COPY_SQL = "COPY metrics (system_id, metric_name, value, collected_at) FROM STDIN"
INSERT_SQL = """INSERT INTO metrics (system_id, metric_name, value, collected_at)
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING"""

CLOSE_ALERT_SQL = """
UPDATE alerts
SET status = CASE WHEN status = 'open' THEN 'cleared' ELSE status END,
    cleared_at = %s,
    last_seen_at = %s,
    occurrence_count = CASE WHEN status = 'flapping' THEN %s ELSE occurrence_count END
WHERE id = %s"""

TOUCH_ALERT_SQL = """
UPDATE alerts
SET last_seen_at = %s,
    occurrence_count = CASE WHEN status = 'flapping' THEN %s ELSE occurrence_count END
WHERE id = %s"""

HEARTBEAT_SQL = """
UPDATE collectors
SET last_heartbeat_at = %s - random() * interval '60 seconds'
WHERE mod(id, 25) <> 0                      -- ~10 collectors stay dead on purpose
  AND NOT (system_id = ANY(%s::int[]))      -- systems with a heartbeat incident"""


def attach_existing_alerts(cur, st, gap_start):
    """Link ongoing incidents to alerts that already exist in the database."""
    cur.execute("""SELECT id, system_id, rule_id, fired_at, cleared_at FROM alerts
                   WHERE cleared_at IS NULL OR cleared_at >= %s""", (gap_start,))
    existing = {(sid, rid, fired): (aid, cleared)
                for aid, sid, rid, fired, cleared in cur.fetchall()}
    for inc in st["active"]:
        key = (inc["system_id"], inc["rule_id"], al.fired_time(inc, st["ctx"]))
        alert_id, cleared_at = existing.get(key, (None, None))
        inc["alert_id"] = alert_id
        inc["closed"] = cleared_at is not None


def start_new_incidents(cur, st, block_start, n_points):
    block_end = block_start + g.STEP * n_points
    busy = {(i["system_id"], i["metric_name"])
            for i in st["incidents"] if i["end"] > block_start}
    new = []
    for _ in range(rng.poisson(g.INCIDENTS_PER_DAY * n_points / g.POINTS_PER_DAY)):
        inc = st["sampler"].sample(block_start + g.STEP * random.randrange(n_points))
        key = (inc["system_id"], inc["metric_name"])
        if key not in busy:
            busy.add(key)
            new.append(inc)
    new += g.maintenance_incidents(cur, st["systems"], block_start, block_end,
                                   starting_only=True)
    for inc in new:
        inc["alert_id"], inc["closed"] = None, False
    st["incidents"] += new
    st["active"] += new


def insert_metrics(cur, st, block_start, n_points, use_copy):
    by_metric = defaultdict(list)
    for inc in st["incidents"]:
        if inc["end"] > block_start:                 # only incidents that can overlap
            by_metric[inc["metric_name"]].append(inc)
    rows = st["gen"].rows(block_start, n_points, by_metric)

    if use_copy:
        count = 0
        with cur.copy(COPY_SQL) as copy:
            for row in rows:
                copy.write_row(row)
                count += 1
        return count
    rows = list(rows)
    cur.executemany(INSERT_SQL, rows)
    return len(rows)


def update_alerts(cur, st, as_of):
    created = cleared = 0
    for inc in st["active"]:
        if inc["closed"]:
            continue
        if inc["alert_id"] is None:
            if al.fired_time(inc, st["ctx"]) <= as_of:
                inc["alert_id"], _ = al.create_alert(cur, inc, st["ctx"], as_of, live=True)
                inc["closed"] = inc["end"] <= as_of
                created += 1
        elif inc["end"] <= as_of:
            cur.execute(CLOSE_ALERT_SQL, (inc["end"], inc["end"],
                                          al.flapping_occurrences(inc, inc["end"]),
                                          inc["alert_id"]))
            inc["closed"] = True
            cleared += 1
        else:
            cur.execute(TOUCH_ALERT_SQL, (as_of, al.flapping_occurrences(inc, as_of),
                                          inc["alert_id"]))
    st["active"] = [i for i in st["active"] if not i["closed"]]
    return created, cleared


def update_heartbeats(cur, st, as_of):
    down = [i["system_id"] for i in st["incidents"]
            if i["metric_name"] == "heartbeat_age_minutes" and i["start"] <= as_of < i["end"]]
    cur.execute(HEARTBEAT_SQL, (as_of, down))


def process_block(conn, st, block_start, n_points, use_copy):
    as_of = block_start + g.STEP * (n_points - 1)    # time of the last point in the block
    with conn.cursor() as cur:
        start_new_incidents(cur, st, block_start, n_points)
        rows = insert_metrics(cur, st, block_start, n_points, use_copy)
        created, cleared = update_alerts(cur, st, as_of)
        update_heartbeats(cur, st, as_of)
    conn.commit()
    g.save_incidents(st["incidents"])
    return rows, created, cleared


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            systems = g.load_systems(cur)
            g.load_rules(cur)
            metrics = g.load_metric_names(cur)
            ctx = al.load_context(cur)
            cur.execute("SELECT max(collected_at) FROM metrics")
            last_ts = cur.fetchone()[0]
            if last_ts is None:
                raise SystemExit("No metrics found. Run backfill_metrics.py first.")

            gap_start = last_ts + g.STEP
            incidents = g.load_incidents()
            st = dict(systems=systems, ctx=ctx, incidents=incidents,
                      sampler=g.IncidentSampler(systems),
                      gen=g.MetricGenerator(systems, metrics, rng),
                      active=[i for i in incidents if i["end"] > gap_start])
            attach_existing_alerts(cur, st, gap_start)
            st["active"] = [i for i in st["active"] if not i["closed"]]
        conn.commit()

        try:
            # 1. catch up from the last stored point to now
            next_t = gap_start
            now_floor = g.floor_to_step(datetime.now(timezone.utc))
            while next_t <= now_floor:
                n = min(g.POINTS_PER_DAY, int((now_floor - next_t) / g.STEP) + 1)
                rows, created, cleared = process_block(conn, st, next_t, n, use_copy=True)
                print(f"Gap fill {(next_t + g.IST):%d %b %H:%M} IST: {n} points, "
                      f"{rows:,} rows, +{created} alerts, {cleared} cleared")
                next_t += g.STEP * n
                now_floor = g.floor_to_step(datetime.now(timezone.utc))

            # 2. live loop
            print("Live mode: one point every 2 minutes. Ctrl+C to stop.")
            while True:
                wait = (next_t - datetime.now(timezone.utc)).total_seconds()
                if wait > 0:
                    time.sleep(wait)
                t0 = time.time()
                rows, created, cleared = process_block(conn, st, next_t, 1, use_copy=False)
                print(f"{(next_t + g.IST):%H:%M} IST: {rows:,} rows, +{created} alerts, "
                      f"{cleared} cleared, {len(st['active'])} active incidents "
                      f"({time.time() - t0:.1f}s)")
                next_t += g.STEP
        except KeyboardInterrupt:
            print("\nStopped. Run again to resume.")


if __name__ == "__main__":
    main()