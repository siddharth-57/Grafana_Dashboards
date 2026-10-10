"""Shared data-generation logic used by backfill_metrics.py and live_ingest.py."""
import json
import math
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np

STEP = timedelta(minutes=2)
POINTS_PER_DAY = 720
INCIDENTS_PER_DAY = 200
IST = timedelta(hours=5, minutes=30)
FS_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)   # fixed reference for filesystem growth
PARAMS_SEED = 1234        # same per-system behaviour in every script and every run

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


def floor_to_step(t):
    """Round a timestamp down to the 2-minute grid."""
    return t.replace(second=0, microsecond=0) - timedelta(minutes=t.minute % 2)


# ---------------------------------------------------------------- loading
def load_systems(cur):
    cur.execute("SELECT id, customer_id, system_type, source FROM systems ORDER BY id")
    return [dict(id=r[0], customer_id=r[1], type=r[2], source=r[3]) for r in cur.fetchall()]


def load_metric_names(cur):
    cur.execute("SELECT name FROM metric_definitions ORDER BY name")
    return [r[0] for r in cur.fetchall()]


def load_rules(cur):
    GLOBAL_RULES.clear()
    CUSTOM_RULES.clear()
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


class IncidentSampler:
    """Picks a random metric, system and matching rule for a new incident."""

    def __init__(self, systems):
        rng = np.random.default_rng(PARAMS_SEED + 1)        # same noisy systems every run
        self.systems = systems
        self.weights = [TYPE_NOISE_WEIGHT[s["type"]] * (rng.pareto(1.5) + 1) for s in systems]
        self.collector_idx = [i for i, s in enumerate(systems) if s["source"] == "collector"]
        self.collector_w = [self.weights[i] for i in self.collector_idx]
        self.all_idx = list(range(len(systems)))
        self.metrics = list(INCIDENT_PROFILES)
        self.metric_w = [INCIDENT_PROFILES[m][0] for m in self.metrics]

    def sample(self, start):
        metric = random.choices(self.metrics, weights=self.metric_w)[0]
        if metric == "heartbeat_age_minutes":
            idx = random.choices(self.collector_idx, weights=self.collector_w)[0]
        else:
            idx = random.choices(self.all_idx, weights=self.weights)[0]
        system = self.systems[idx]
        candidates = candidate_rules(system["customer_id"], metric)
        rule = random.choices(candidates,
                              weights=[SEVERITY_WEIGHT[r["severity"]] for r in candidates])[0]
        _, dmin, dmax = INCIDENT_PROFILES[metric]
        duration = max(random.randint(dmin, dmax), rule["duration"] + 6)
        lo, hi = value_band(metric, rule, candidates)
        flapping = metric in FLAPPABLE and random.random() < 0.08
        return make_incident(system, metric, rule, start,
                             start + timedelta(minutes=duration), lo, hi, flapping)


def remove_overlaps(incidents):
    """Keep at most one incident at a time per system + metric."""
    incidents = sorted(incidents, key=lambda x: (x["system_id"], x["metric_name"], x["start"]))
    kept, last_end = [], {}
    for inc in incidents:
        key = (inc["system_id"], inc["metric_name"])
        if key in last_end and inc["start"] < last_end[key] + timedelta(minutes=10):
            continue
        kept.append(inc)
        last_end[key] = inc["end"]
    return kept


def maintenance_incidents(cur, systems, start, end, starting_only=False):
    """Maintenance windows as 'system down' incidents.
    starting_only=True: windows that START in [start, end) (used by live mode)."""
    if starting_only:
        cur.execute("""SELECT system_id, start_at, end_at FROM maintenance_windows
                       WHERE start_at >= %s AND start_at < %s""", (start, end))
    else:
        cur.execute("""SELECT system_id, start_at, end_at FROM maintenance_windows
                       WHERE start_at < %s AND end_at > %s""", (end, start))
    by_id = {s["id"]: s for s in systems}
    out = []
    for system_id, s_at, e_at in cur.fetchall():
        system = by_id[system_id]
        rule = candidate_rules(system["customer_id"], "system_available")[0]
        out.append(make_incident(system, "system_available", rule, s_at, e_at,
                                 0.0, 0.0, maintenance=True))
    return out


def save_incidents(incidents, path="incidents.json"):
    with open(path, "w") as f:
        json.dump(incidents, f, indent=1, default=lambda o: o.isoformat())


def load_incidents(path="incidents.json"):
    with open(path) as f:
        incidents = json.load(f)
    for inc in incidents:
        inc["start"] = datetime.fromisoformat(inc["start"])
        inc["end"] = datetime.fromisoformat(inc["end"])
    return incidents


# ---------------------------------------------------------------- metric values
def system_params(systems):
    rng = np.random.default_rng(PARAMS_SEED)                 # fixed baselines per system
    n = len(systems)
    return dict(type=np.array([TYPE_FACTOR[s["type"]] for s in systems]),
                cpu=rng.uniform(10, 30, n), mem=rng.uniform(50, 70, n),
                fs_start=rng.uniform(40, 70, n), fs_growth=rng.uniform(0.1, 0.6, n),
                resp=rng.uniform(300, 650, n))


def time_features(ts_list):
    """Business-hour load curve (IST, lighter weekends) and days since FS_EPOCH."""
    local = [t + IST for t in ts_list]
    hour = np.array([t.hour + t.minute / 60 for t in local])
    weekend = np.array([t.weekday() >= 5 for t in local])
    load = np.clip(np.sin(np.pi * (hour - 7) / 12), 0, None) * np.where(weekend, 0.3, 1.0)
    fs_days = np.array([(t - FS_EPOCH) / timedelta(days=1) for t in ts_list])
    return load, fs_days


def normal_values(metric, p, load, fs_days, rng):
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
        grown = np.mod(col("fs_growth") * fs_days[None, :], span)
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


def overlay(matrix, row_of, incidents, block_start, n_points, rng):
    """Write incident values over the normal values for this time block."""
    block_end = block_start + STEP * n_points
    for inc in incidents:
        if inc["end"] <= block_start or inc["start"] >= block_end:
            continue
        row = row_of[inc["system_id"]]
        i0 = max(0, math.ceil((inc["start"] - block_start) / STEP))
        i1 = min(n_points, math.ceil((inc["end"] - block_start) / STEP))
        if i1 <= i0:
            continue
        idx = np.arange(i0, i1)
        if inc["metric_name"] == "heartbeat_age_minutes":     # age keeps growing
            vals = 6 + idx * 2 + (block_start - inc["start"]) / timedelta(minutes=1)
        else:
            vals = rng.uniform(inc["lo"], inc["hi"], len(idx))
        if inc["metric_name"].endswith("_count"):
            vals = np.round(vals)
        segment = matrix[row, i0:i1]                           # view into matrix
        if inc["flapping"]:
            # breach / recover pattern, aligned to the incident start
            offset = round((block_start - inc["start"]) / STEP)
            mask = ((idx + offset) // 2) % 2 == 0
            segment[mask] = vals[mask]
        else:
            segment[:] = vals


class MetricGenerator:
    """Produces (system_id, metric_name, value, collected_at) rows for a time block."""

    def __init__(self, systems, metrics, rng):
        self.systems, self.metrics, self.rng = systems, metrics, rng
        self.params = system_params(systems)
        self.rows_for = {m: [i for i, s in enumerate(systems)
                             if m != "heartbeat_age_minutes" or s["source"] == "collector"]
                         for m in metrics}

    def rows(self, block_start, n_points, incidents_by_metric):
        ts_list = [block_start + k * STEP for k in range(n_points)]
        load, fs_days = time_features(ts_list)
        for metric in self.metrics:
            idx = self.rows_for[metric]
            matrix = normal_values(metric, self.params, load, fs_days, self.rng)[idx]
            sys_ids = [self.systems[i]["id"] for i in idx]
            row_of = {sid: r for r, sid in enumerate(sys_ids)}
            overlay(matrix, row_of, incidents_by_metric.get(metric, []),
                    block_start, n_points, self.rng)
            for sid, values in zip(sys_ids, np.round(matrix, 2).tolist()):
                for ts, v in zip(ts_list, values):
                    yield (sid, metric, v, ts)