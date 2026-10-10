"""Shared alert + notification logic used by backfill_alerts.py and live_ingest.py."""
import random
from bisect import bisect_right
from collections import defaultdict
from datetime import timedelta

AVAILABILITY_METRICS = {"instance_available", "system_available", "db_available",
                        "db_replication_in_sync", "heartbeat_age_minutes"}
NOTIFIED_STATUSES = {"open", "cleared", "flapping"}

MAINTENANCE_REASON = "In maintenance window"
DEV_REASON = "Low priority: DEV system"
FLAPPING_REASON = "Threshold crossed repeatedly"
SUSPECTED_FALSE_REASONS = ["Correlated metrics normal", "Single-point spike",
                           "Known batch window"]

# channel: (failure rate, possible error messages)
FAILURES = {
    "email": (0.01, ["Mailbox full", "SMTP timeout"]),
    "sms":   (0.03, ["Invalid phone number", "Carrier rejected message"]),
    "call":  (0.05, ["No answer", "Line busy"]),
    "teams": (0.02, ["Webhook returned 429", "Webhook timeout"]),
}

GUIDANCE = {
    "cpu_usage_pct": "Likely cause: long-running reports or batch jobs. Check SM50/SM66 for busy work processes and ST06 for top OS processes.",
    "memory_usage_pct": "Likely cause: memory-heavy programs or a leak. Check ST02 buffers and ST06 memory usage.",
    "filesystem_usage_pct": "Likely cause: growing logs or traces. Check large directories, archive or delete old files.",
    "free_dialog_wp_pct": "Likely cause: hung or long-running dialog steps. Check SM50/SM66 and cancel stuck processes if safe.",
    "dialog_response_time_ms": "Likely cause: overloaded instance or slow DB. Check ST03N workload and DB02 for expensive statements.",
    "update_response_time_ms": "Likely cause: update queue backlog. Check SM13 for pending or failed updates.",
    "abap_short_dumps_count": "Likely cause: program errors after a change. Check ST22 for the most frequent dump types.",
    "failed_batch_jobs_count": "Likely cause: job input or authorisation errors. Check SM37 job logs.",
    "instance_available": "Instance is down. Check the instance status and its work process traces.",
    "system_available": "System is unreachable. Check whether all instances and the message server are up.",
    "db_available": "Database is down. Check DB logs and the DB host status immediately.",
    "db_replication_in_sync": "Replication is lagging or broken. Check replication status and network between DB nodes.",
    "heartbeat_age_minutes": "Collector stopped reporting. Check the collector service and network on the host.",
}

ALERT_INSERT = """
INSERT INTO alerts (system_id, rule_id, alert_name, alert_type, metric_name,
                    metric_value, threshold_value, severity, status, status_reason,
                    occurrence_count, fired_at, last_seen_at, cleared_at, llm_guidance)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING id"""

NOTIFICATION_INSERT = """
INSERT INTO notifications (alert_id, contact_id, group_id, channel,
                           status, error_message, sent_at)
VALUES (%s, %s, %s, %s, %s, %s, %s)"""


def load_context(cur):
    """Everything needed to turn incidents into alerts, loaded once."""
    cur.execute("""SELECT s.id, s.system_type, c.id, c.group_id
                   FROM systems s JOIN customers c ON c.id = s.customer_id""")
    systems = {sid: dict(type=t, customer_id=cid, group_id=gid)
               for sid, t, cid, gid in cur.fetchall()}

    cur.execute("SELECT id, alert_name, alert_type, duration_minutes FROM rules")
    rules = {rid: dict(name=n, type=t, duration=d) for rid, n, t, d in cur.fetchall()}

    cur.execute("""SELECT group_id, start_at, contact_id FROM oncall_schedules
                   ORDER BY group_id, start_at""")
    oncall = defaultdict(list)
    for gid, start, contact in cur.fetchall():
        oncall[gid].append((start, contact))

    cur.execute("""SELECT customer_id, id FROM contacts
                   WHERE contact_type = 'ecs' AND is_active""")
    ecs = dict(cur.fetchall())
    return dict(systems=systems, rules=rules, oncall=oncall, ecs=ecs)


def fired_time(inc, ctx):
    """An alert fires once the breach has lasted the rule's duration."""
    return inc["start"] + timedelta(minutes=ctx["rules"][inc["rule_id"]]["duration"])


def on_call(ctx, group_id, t):
    shifts = ctx["oncall"][group_id]
    i = bisect_right([s for s, _ in shifts], t) - 1
    return shifts[max(i, 0)][1]


def decide_status(inc, system):
    if inc["maintenance"]:
        return "suppressed", MAINTENANCE_REASON
    if inc["flapping"]:
        return "flapping", FLAPPING_REASON
    if system["type"] == "DEV" and inc["severity"] != "critical" and random.random() < 0.3:
        return "suppressed", DEV_REASON
    if inc["metric_name"] not in AVAILABILITY_METRICS and random.random() < 0.12:
        return "suspected_false", random.choice(SUSPECTED_FALSE_REASONS)
    return None, None                                  # normal open / cleared lifecycle


def alert_value(inc):
    if inc["metric_name"] == "heartbeat_age_minutes":
        return 6.0
    if inc["operator"] == "<":
        return inc["lo"]
    value = (inc["lo"] + inc["hi"]) / 2
    return float(round(value)) if inc["metric_name"].endswith("_count") else round(value, 2)


def flapping_occurrences(inc, until):
    """One breach roughly every 8 minutes (2 points on, 2 points off)."""
    return max(2, int((until - inc["start"]) / timedelta(minutes=8)))


def build_notifications(alert_id, severity, system, fired_at, ctx, now=None):
    """now=None (live mode): send everything and decide outcomes immediately.
    now=<time> (backfill): skip future sends, mark the last 2 minutes as 'sent'."""
    s = lambda lo, hi: fired_at + timedelta(seconds=random.randint(lo, hi))
    consultant = on_call(ctx, system["group_id"], fired_at)
    planned = [(None, system["group_id"], "teams", s(5, 30))]
    if severity == "critical":
        planned += [(consultant, None, "sms", s(10, 40)),
                    (consultant, None, "call", s(120, 180)),
                    (ctx["ecs"][system["customer_id"]], None, "email", s(20, 90))]
    elif severity == "warning":
        planned.append((consultant, None, "email", s(10, 60)))

    rows = []
    for contact_id, group_id, channel, sent_at in planned:
        if now is not None and sent_at > now:
            continue
        rate, errors = FAILURES[channel]
        if now is not None and sent_at > now - timedelta(minutes=2):
            status, error = "sent", None
        elif random.random() < rate:
            status, error = "failed", random.choice(errors)
        else:
            status, error = "delivered", None
        rows.append((alert_id, contact_id, group_id, channel, status, error, sent_at))
    return rows


def create_alert(cur, inc, ctx, now, live=False):
    """Insert the alert for an incident plus its notifications. Returns (alert_id, status)."""
    rule = ctx["rules"][inc["rule_id"]]
    system = ctx["systems"][inc["system_id"]]
    fired_at = fired_time(inc, ctx)

    status, reason = decide_status(inc, system)
    ended = inc["end"] <= now
    status = status or ("cleared" if ended else "open")
    cleared_at = inc["end"] if ended else None
    last_seen = max(fired_at, min(inc["end"], now))
    occurrences = flapping_occurrences(inc, last_seen) if status == "flapping" else 1
    notified = status in NOTIFIED_STATUSES
    guidance = (GUIDANCE[inc["metric_name"]]
                if notified and inc["severity"] == "critical" else None)

    cur.execute(ALERT_INSERT,
                (inc["system_id"], inc["rule_id"], rule["name"], rule["type"],
                 inc["metric_name"], alert_value(inc), inc["threshold"], inc["severity"],
                 status, reason, occurrences, fired_at, last_seen, cleared_at, guidance))
    alert_id = cur.fetchone()[0]

    if notified:
        rows = build_notifications(alert_id, inc["severity"], system, fired_at, ctx,
                                   now=None if live else now)
        cur.executemany(NOTIFICATION_INSERT, rows)
    return alert_id, status