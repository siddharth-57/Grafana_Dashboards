"""Step 4: backfill the last 30 days of metrics (logic lives in generator.py)."""
import random
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np

import generator as g
from db import get_connection

random.seed(7)
rng = np.random.default_rng(7)

BACKFILL_DAYS = 30
END = g.floor_to_step(datetime.now(timezone.utc))
START = END - timedelta(days=BACKFILL_DAYS)


def plan_incidents(cur, systems):
    sampler = g.IncidentSampler(systems)
    slots = BACKFILL_DAYS * g.POINTS_PER_DAY
    planned = [sampler.sample(START + g.STEP * random.randrange(slots))
               for _ in range(g.INCIDENTS_PER_DAY * BACKFILL_DAYS)]
    return g.remove_overlaps(planned) + g.maintenance_incidents(cur, systems, START, END)


def main():
    t0 = time.time()
    with get_connection() as conn:
        with conn.cursor() as cur:
            systems = g.load_systems(cur)
            g.load_rules(cur)
            metrics = g.load_metric_names(cur)
            incidents = plan_incidents(cur, systems)
            cur.execute("TRUNCATE metrics")
        conn.commit()
        g.save_incidents(incidents)
        print(f"Planned {len(incidents)} incidents -> incidents.json")

        by_metric = defaultdict(list)
        for inc in incidents:
            by_metric[inc["metric_name"]].append(inc)
        gen = g.MetricGenerator(systems, metrics, rng)

        for d in range(BACKFILL_DAYS):
            day_start = START + timedelta(days=d)
            rows_written = 0
            with conn.cursor() as cur:
                with cur.copy("COPY metrics (system_id, metric_name, value, collected_at) "
                              "FROM STDIN") as copy:
                    for row in gen.rows(day_start, g.POINTS_PER_DAY, by_metric):
                        copy.write_row(row)
                        rows_written += 1
            conn.commit()
            print(f"Day {d + 1:2}/{BACKFILL_DAYS} {day_start:%Y-%m-%d}: "
                  f"{rows_written:,} rows  ({time.time() - t0:.0f}s)")

    print(f"Done in {(time.time() - t0) / 60:.1f} min")


if __name__ == "__main__":
    main()