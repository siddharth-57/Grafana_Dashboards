"""Step 4: backfill the last 30 days of metrics.

1. Plans incidents and saves them to incidents.json (Step 5 builds alerts from it).
2. Generates normal behaviour that always stays below alert thresholds.
3. Overlays incidents, then loads each day with COPY.
"""
import json
import math
import random
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np

from db import get_connection

random.seed(7)
rng = np.random.default_rng(7)

BACKFILL_DAYS = 30
STEP = timedelta(minutes=2)
POINTS_PER_DAY = 720
INCIDENTS_PER_DAY = 200
IST = timedelta(hours=5, minutes=30)

NOW = datetime.now(timezone.utc)
END = NOW.replace(second=0, microsecond=0) - timedelta(minutes=NOW.minute % 2)
START = END - timedelta(days=BACKFILL_DAYS)

TYPE_FACTOR = {"PROD": 1.0, "QA": 0.5, "DEV": 0.3}        # how loaded a system is
TYPE_NOISE_WEIGHT = {"PROD": 3.0, "QA": 1.5, "DEV": 1.0}  # how often it has incidents
SEVERITY_WEIGHT = {"warning": 0.6, "critical": 0.25, "info": 0.15}
STATUS_METRICS = {"instance_available", "system_available",
                  "db_available", "db_replication_in_sync"}
FLAPPABLE = {"cpu_usage_pct", "memory_usage_pct",
             "dialog_response_time_ms", "free_dialog_wp_pct"}

# metric: (relative frequency, min duration, max duration) in minutes
INCIDENT_PROFILES = {
    "cpu_usage_pct":           (20, 15, 90),
    "memory_usage_pct":        (10, 15, 120),
    "filesystem_usage_pct":    (8, 120, 720),
    "free_dialog_wp_pct":      (8, 10, 45),
    "dialog_response_time_ms": (15, 15, 60),
    "update_response_time_ms": (5, 15, 60),
    "abap_short_dumps_count":  (10, 10, 40),
    "failed_batch_jobs_count": (15, 2, 10),
    "instance_available":      (2, 6, 40),
    "system_available":        (1, 6, 30),
    "db_available":            (1, 6, 30),
    "db_replication_in_sync":  (2, 10, 60),
    "heartbeat_age_minutes":   (3, 10, 90),
}

GLOBAL_RULES = defaultdict(list)   # metric -> [rule]
CUSTOM_RULES = defaultdict(list)   # (customer_id, metric) -> [rule]


# ---------------------------------------------------------------- loading
def load_systems(cur):
    cur.execute("SELECT id, customer_id, system_type, source FROM systems ORDER BY id")
    return [dict(id=r[0], customer_id=r[1], type=r[2], source=r[3]) for r in cur.fetchall()]


def load_rules(cur):
    cur.execute("""SELECT id, customer_id, metric_name, operator, threshold,
                          duration_minutes, severity
                   FROM rules WHERE is_active""")
    for rid, cid, metric, op, thr, dur, sev in cur.fetchall():
        rule = dict(id=rid, op=op, threshold=thr, duration=dur, severity=sev)
        if cid is None:
            GLOBAL_RULES[metric].append(rule)
        else:
            CUSTOM_RULES[(cid, metric)].append(rule)


def candidate_rules(customer_id, metric):
    """Customer rules override global rules of the same severity."""
    custom = CUSTOM_RULES.get((customer_id, metric), [])
    covered = {r["severity"] for r in custom}
    return custom + [r for r in GLOBAL_RULES[metric] if r["severity"] not in covered]


# ---------------------------------------------------------------- incidents
def value_band(metric, rule, candidates):
    """Value range that breaches this rule but not a higher-severity one."""
    thr = rule["threshold"]
    if rule["op"] == "<":
        return (0.0, 0.0) if thr <= 1 else (1.0, thr - 1)
    lo = thr + 1 if thr <= 20 else thr * 1.02
    higher = [r["threshold"] for r in candidates if r["op"] == ">" and r["threshold"] > thr]
    hi = min(higher) - 0.5 if higher else thr * 1.3 + 2
    if metric.endswith("_pct"):
        hi = min(hi, 100.0)
    return lo, hi


def make_incident(system, metric, rule, start, end, lo, hi, flapping=False, maintenance=False):
    return dict(system_id=system["id"], customer_id=system["customer_id"],
                metric_name=metric, rule_id=rule["id"], severity=rule["severity"],
                operator=rule["op"], threshold=rule["threshold"],
                start=start, end=end, lo=lo, hi=hi,
                flapping=flapping, maintenance=maintenance)


def plan_incidents(cur, systems):
    weights = [TYPE_NOISE_WEIGHT[s["type"]] * (rng.pareto(1.5) + 1) for s in systems]
    collector_idx = [i for i, s in enumerate(systems) if s["source"] == "collector"]
    collector_w = [weights[i] for i in collector_idx]
    all_idx = list(range(len(systems)))
    metrics = list(INCIDENT_PROFILES)
    metric_w = [INCIDENT_PROFILES[m][0] for m in metrics]
    slots = BACKFILL_DAYS * POINTS_PER_DAY

    planned = []
    for _ in range(INCIDENTS_PER_DAY * BACKFILL_DAYS):
        metric = random.choices(metrics, weights=metric_w)[0]
        if metric == "heartbeat_age_minutes":
            idx = random.choices(collector_idx, weights=collector_w)[0]
        else:
            idx = random.choices(all_idx, weights=weights)[0]
        system = systems[idx]
        candidates = candidate_rules(system["customer_id"], metric)
        rule = random.choices(candidates,
                              weights=[SEVERITY_WEIGHT[r["severity"]] for r in candidates])[0]
        _, dmin, dmax = INCIDENT_PROFILES[metric]
        duration = max(random.randint(dmin, dmax), rule["duration"] + 6)
        start = START + STEP * random.randrange(slots)
        lo, hi = value_band(metric, rule, candidates)
        flapping = metric in FLAPPABLE and random.random() < 0.08
        planned.append(make_incident(system, metric, rule, start,
                                     start + timedelta(minutes=duration), lo, hi, flapping))

    # drop overlapping incidents on the same system + metric
    planned.sort(key=lambda x: (x["system_id"], x["metric_name"], x["start"]))
    incidents, last_end = [], {}
    for inc in planned:
        key = (inc["system_id"], inc["metric_name"])
        if key in last_end and inc["start"] < last_end[key] + timedelta(minutes=10):
            continue
        incidents.append(inc)
        last_end[key] = inc["end"]

    # every maintenance window in range = planned downtime (system down)
    by_id = {s["id"]: s for s in systems}
    cur.execute("""SELECT system_id, start_at, end_at FROM maintenance_windows
                   WHERE start_at < %s AND end_at > %s""", (END, START))
    for system_id, s_at, e_at in cur.fetchall():
        system = by_id[system_id]
        rule = candidate_rules(system["customer_id"], "system_available")[0]
        incidents.append(make_incident(system, "system_available", rule, s_at, e_at,
                                       0.0, 0.0, maintenance=True))
    return incidents


def save_incidents(incidents):
    with open("incidents.json", "w") as f:
        json.dump(incidents, f, indent=1, default=lambda o: o.isoformat())


# ---------------------------------------------------------------- normal behaviour
def system_params(systems):
    n = len(systems)
    return dict(type=np.array([TYPE_FACTOR[s["type"]] for s in systems]),
                cpu=rng.uniform(10, 30, n), mem=rng.uniform(50, 70, n),
                fs_start=rng.uniform(40, 70, n), fs_growth=rng.uniform(0.1, 0.6, n),
                resp=rng.uniform(300, 650, n))


def normal_values(metric, p, load, days_elapsed):
    """Values for ALL systems, shape (systems, points), clipped below thresholds."""
    L = p["type"][:, None] * load[None, :]
    noise = lambda sd: rng.normal(0, sd, L.shape)
    col = lambda key: p[key][:, None]

    if metric == "cpu_usage_pct":
        return np.clip(col("cpu") + 45 * L + noise(3), 1, 78)
    if metric == "memory_usage_pct":
        return np.clip(col("mem") + 12 * L + noise(1.5), 20, 88)
    if metric == "filesystem_usage_pct":                     # slow growth + cleanups
        span = (80 - p["fs_start"])[:, None]
        grown = np.mod(col("fs_growth") * days_elapsed[None, :], span)
        return np.clip(col("fs_start") + grown + noise(0.2), 5, 82)
    if metric == "free_dialog_wp_pct":
        return np.clip(85 - 55 * L + noise(4), 15, 100)
    if metric == "dialog_response_time_ms":
        return np.clip(col("resp") * (1 + 0.8 * L) + noise(40), 100, 1400)
    if metric == "update_response_time_ms":
        return np.clip(col("resp") * 1.3 * (1 + 0.6 * L) + noise(60), 150, 1800)
    if metric == "abap_short_dumps_count":
        return np.minimum(rng.poisson(1 + 6 * L), 15).astype(float)
    if metric == "failed_batch_jobs_count":
        return np.zeros(L.shape)
    if metric in STATUS_METRICS:
        return np.ones(L.shape)
    if metric == "heartbeat_age_minutes":
        return rng.uniform(0, 2, L.shape)
    raise ValueError(f"No generator for metric {metric}")


def overlay(matrix, row_of, incidents, day_start):
    day_end = day_start + timedelta(days=1)
    for inc in incidents:
        if inc["end"] <= day_start or inc["start"] >= day_end:
            continue
        row = row_of[inc["system_id"]]
        i0 = max(0, math.ceil((inc["start"] - day_start) / STEP))
        i1 = min(POINTS_PER_DAY, math.ceil((inc["end"] - day_start) / STEP))
        if i1 <= i0:
            continue
        idx = np.arange(i0, i1)
        if inc["metric_name"] == "heartbeat_age_minutes":     # age keeps growing
            vals = 6 + idx * 2 + (day_start - inc["start"]) / timedelta(minutes=1)
        else:
            vals = rng.uniform(inc["lo"], inc["hi"], len(idx))
        if inc["metric_name"].endswith("_count"):
            vals = np.round(vals)
        segment = matrix[row, i0:i1]                           # view into matrix
        if inc["flapping"]:
            mask = (idx // 2) % 2 == 0                         # breach, recover, breach...
            segment[mask] = vals[mask]
        else:
            segment[:] = vals


# ---------------------------------------------------------------- main
def main():
    t0 = time.time()
    with get_connection() as conn:
        with conn.cursor() as cur:
            systems = load_systems(cur)
            load_rules(cur)
            cur.execute("SELECT name FROM metric_definitions ORDER BY name")
            metrics = [r[0] for r in cur.fetchall()]
            incidents = plan_incidents(cur, systems)
            cur.execute("TRUNCATE metrics")
        conn.commit()
        save_incidents(incidents)
        print(f"Planned {len(incidents)} incidents -> incidents.json")

        incidents_by_metric = defaultdict(list)
        for inc in incidents:
            incidents_by_metric[inc["metric_name"]].append(inc)

        params = system_params(systems)
        rows_for = {m: [i for i, s in enumerate(systems)
                        if m != "heartbeat_age_minutes" or s["source"] == "collector"]
                    for m in metrics}

        for d in range(BACKFILL_DAYS):
            day_start = START + timedelta(days=d)
            ts_list = [day_start + k * STEP for k in range(POINTS_PER_DAY)]
            local = [t + IST for t in ts_list]
            hour = np.array([t.hour + t.minute / 60 for t in local])
            weekend = np.array([t.weekday() >= 5 for t in local])
            load = np.clip(np.sin(np.pi * (hour - 7) / 12), 0, None) * np.where(weekend, 0.3, 1.0)
            days_elapsed = np.array([(t - START) / timedelta(days=1) for t in ts_list])

            rows_written = 0
            with conn.cursor() as cur:
                with cur.copy("COPY metrics (system_id, metric_name, value, collected_at) "
                              "FROM STDIN") as copy:
                    for metric in metrics:
                        idx = rows_for[metric]
                        matrix = normal_values(metric, params, load, days_elapsed)[idx]
                        sys_ids = [systems[i]["id"] for i in idx]
                        row_of = {sid: r for r, sid in enumerate(sys_ids)}
                        overlay(matrix, row_of, incidents_by_metric[metric], day_start)
                        for sid, values in zip(sys_ids, np.round(matrix, 2).tolist()):
                            for ts, v in zip(ts_list, values):
                                copy.write_row((sid, metric, v, ts))
                        rows_written += len(sys_ids) * POINTS_PER_DAY
            conn.commit()
            print(f"Day {d + 1:2}/{BACKFILL_DAYS} {day_start:%Y-%m-%d}: "
                  f"{rows_written:,} rows  ({time.time() - t0:.0f}s)")

    print(f"Done in {(time.time() - t0) / 60:.1f} min")


if __name__ == "__main__":
    main()