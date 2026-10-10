"""Seed static data: groups, customers, systems, collectors, rules,
contacts, on-call schedules, maintenance windows, users."""
import hashlib
import random
import secrets
import string
from datetime import datetime, timedelta, timezone

from faker import Faker
from psycopg.types.json import Jsonb

from db import get_connection

random.seed(42)
Faker.seed(42)
fake = Faker()

NOW = datetime.now(timezone.utc)
NUM_CUSTOMERS = 108
INITIAL_CUSTOMERS = 73          # older customers; the rest joined recently
CONSULTANTS_PER_GROUP = 6
HISTORY_DAYS = 30               # how far back schedules / maintenance go
FUTURE_DAYS = 14                # how far ahead they go

GROUP_NAMES = ["Team Alpha", "Team Beta", "Team Gamma",
               "Team Delta", "Team Epsilon", "Team Zeta"]
GROUP_WEIGHTS = [25, 20, 20, 15, 12, 8]          # uneven customer split
DEPLOYMENTS = ["on_prem", "private_cloud", "saas"]
DEPLOYMENT_WEIGHTS = [40, 40, 20]
COLLECTOR_VERSIONS = ["2.3.0", "2.4.1", "2.5.0"]
VERSION_WEIGHTS = [10, 30, 60]
MAINTENANCE_REASONS = ["Kernel upgrade", "Support package update", "OS patching",
                       "Database upgrade", "Hardware maintenance"]

# (metric, operator, threshold, duration_min, alert_name, alert_type, severity)
GLOBAL_RULES = [
    ("heartbeat_age_minutes",   ">", 5,    0,  "Collector heartbeat missing",    "Availability", "critical"),
    ("cpu_usage_pct",           ">", 85,   10, "High CPU usage",                 "Performance",  "warning"),
    ("cpu_usage_pct",           ">", 95,   5,  "Critical CPU usage",             "Performance",  "critical"),
    ("memory_usage_pct",        ">", 90,   10, "High memory usage",              "Performance",  "warning"),
    ("filesystem_usage_pct",    ">", 85,   0,  "File system filling up",         "Capacity",     "warning"),
    ("filesystem_usage_pct",    ">", 95,   0,  "File system almost full",        "Capacity",     "critical"),
    ("instance_available",      "<", 1,    0,  "ABAP instance down",             "Availability", "critical"),
    ("free_dialog_wp_pct",      "<", 10,   5,  "Few free dialog work processes", "Performance",  "warning"),
    ("system_available",        "<", 1,    0,  "ABAP system down",               "Availability", "critical"),
    ("dialog_response_time_ms", ">", 1500, 10, "Slow dialog response",           "Performance",  "warning"),
    ("dialog_response_time_ms", ">", 3000, 5,  "Very slow dialog response",      "Performance",  "critical"),
    ("update_response_time_ms", ">", 2000, 10, "Slow update response",           "Performance",  "warning"),
    ("abap_short_dumps_count",  ">", 20,   0,  "Many ABAP short dumps",          "Exceptions",   "warning"),
    ("failed_batch_jobs_count", ">", 0,    0,  "Batch job failed",               "Exceptions",   "info"),
    ("failed_batch_jobs_count", ">", 5,    0,  "Many batch jobs failed",         "Exceptions",   "warning"),
    ("db_available",            "<", 1,    0,  "Database down",                  "Availability", "critical"),
    ("db_replication_in_sync",  "<", 1,    5,  "DB replication out of sync",     "Availability", "critical"),
]

TRUNCATE_SQL = """
TRUNCATE groups, customers, systems, collectors, rules, alerts, notifications,
         contacts, oncall_schedules, maintenance_windows, users, audit_log, metrics
RESTART IDENTITY CASCADE;
"""


def random_phone():
    return f"+91{random.randint(7000000000, 9999999999)}"


def insert_returning_id(cur, sql, params):
    cur.execute(sql + " RETURNING id", params)
    return cur.fetchone()[0]


def seed_groups(cur):
    ids = []
    for name in GROUP_NAMES:
        vault = f"secret/teams/{name.lower().replace(' ', '_')}"
        ids.append(insert_returning_id(
            cur, "INSERT INTO groups (name, teams_webhook_vault_path) VALUES (%s, %s)",
            (name, vault)))
    return ids


def seed_customers_systems_collectors(cur, group_ids):
    customer_ids, system_ids = [], []
    for i in range(NUM_CUSTOMERS):
        name = fake.unique.company()
        alias = "".join(w[0] for w in name.replace(",", "").split()[:3]).upper()
        cust_no = f"CUST-{10001 + i}"
        age_days = random.randint(200, 900) if i < INITIAL_CUSTOMERS else random.randint(1, 180)
        group_id = random.choices(group_ids, weights=GROUP_WEIGHTS)[0]

        customer_id = insert_returning_id(
            cur,
            """INSERT INTO customers (group_id, customer_number, alias, name, created_at)
               VALUES (%s, %s, %s, %s, %s)""",
            (group_id, cust_no, alias, name, NOW - timedelta(days=age_days)))
        customer_ids.append(customer_id)

        deployment = random.choices(DEPLOYMENTS, weights=DEPLOYMENT_WEIGHTS)[0]
        source = "cloud_alm" if deployment == "saas" else "collector"
        sid_base = "".join(random.choices(string.ascii_uppercase, k=2))

        for system_type in ("PROD", "QA", "DEV"):
            sid = sid_base + system_type[0]                  # e.g. ECP / ECQ / ECD
            config = (Jsonb({"tenant": f"{alias.lower()}-{i}.alm.example.cloud"})
                      if source == "cloud_alm" else None)
            system_id = insert_returning_id(
                cur,
                """INSERT INTO systems (customer_id, sid, alias, system_type,
                                        deployment_type, source, vault_path, config)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (customer_id, sid, f"{alias} {system_type}", system_type,
                 deployment, source, f"secret/systems/{cust_no}/{sid}", config))
            system_ids.append(system_id)

            if source == "collector":
                token = secrets.token_hex(16)
                stale = random.random() < 0.03
                heartbeat = (NOW - timedelta(minutes=random.randint(10, 600)) if stale
                             else NOW - timedelta(seconds=random.randint(5, 110)))
                cur.execute(
                    """INSERT INTO collectors (system_id, hostname, token_hash, config,
                                               last_heartbeat_at, version)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (system_id, f"{sid.lower()}-{cust_no.lower()}.example.local",
                     hashlib.sha256(token.encode()).hexdigest(),
                     Jsonb({"interval_seconds": 120}), heartbeat,
                     random.choices(COLLECTOR_VERSIONS, weights=VERSION_WEIGHTS)[0]))
    return customer_ids, system_ids


def seed_rules(cur, customer_ids):
    sql = """INSERT INTO rules (customer_id, system_id, metric_name, operator, threshold,
                                duration_minutes, alert_name, alert_type, severity)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    for rule in GLOBAL_RULES:
        cur.execute(sql, (None, None, *rule))
    for customer_id in random.sample(customer_ids, 15):     # customer-specific overrides
        cur.execute(sql, (customer_id, None, "cpu_usage_pct", ">", 80, 10,
                          "High CPU usage (custom)", "Performance", "warning"))


def seed_contacts(cur, group_ids, customer_ids):
    consultants_by_group = {}
    for group_id in group_ids:
        consultants_by_group[group_id] = [
            insert_returning_id(
                cur,
                """INSERT INTO contacts (group_id, name, email, phone, contact_type)
                   VALUES (%s, %s, %s, %s, 'consultant')""",
                (group_id, fake.name(), fake.email(), random_phone()))
            for _ in range(CONSULTANTS_PER_GROUP)]

    for customer_id in customer_ids:
        cur.execute(
            """INSERT INTO contacts (customer_id, name, email, phone, contact_type)
               VALUES (%s, %s, %s, %s, 'ecs')""",
            (customer_id, fake.name(), fake.email(), random_phone()))
        for _ in range(2):
            cur.execute(
                """INSERT INTO contacts (customer_id, name, email, contact_type)
                   VALUES (%s, %s, %s, 'customer')""",
                (customer_id, fake.name(), fake.email()))
    return consultants_by_group


def seed_oncall(cur, consultants_by_group):
    start = (NOW - timedelta(days=HISTORY_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_limit = NOW + timedelta(days=FUTURE_DAYS)
    for group_id, consultants in consultants_by_group.items():
        t, i = start, 0
        while t < end_limit:                                 # weekly rotation
            cur.execute(
                """INSERT INTO oncall_schedules (group_id, contact_id, start_at, end_at)
                   VALUES (%s, %s, %s, %s)""",
                (group_id, consultants[i % len(consultants)], t, t + timedelta(days=7)))
            t += timedelta(days=7)
            i += 1


def seed_maintenance(cur, system_ids):
    sql = """INSERT INTO maintenance_windows (system_id, start_at, end_at, reason)
             VALUES (%s, %s, %s, %s)"""
    for _ in range(80):                                      # past and upcoming windows
        day = random.randint(-HISTORY_DAYS, FUTURE_DAYS)
        start = (NOW + timedelta(days=day)).replace(
            hour=random.choice([18, 20, 22]), minute=0, second=0, microsecond=0)
        cur.execute(sql, (random.choice(system_ids), start,
                          start + timedelta(hours=random.randint(2, 8)),
                          random.choice(MAINTENANCE_REASONS)))
    for system_id in random.sample(system_ids, 3):           # windows active right now
        cur.execute(sql, (system_id, NOW - timedelta(hours=1), NOW + timedelta(hours=3),
                          random.choice(MAINTENANCE_REASONS)))


def seed_users(cur):
    for role, count in (("admin", 3), ("management", 2)):
        for _ in range(count):
            cur.execute("INSERT INTO users (email, role) VALUES (%s, %s)",
                        (fake.unique.email(), role))


def main():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(TRUNCATE_SQL)
        group_ids = seed_groups(cur)
        customer_ids, system_ids = seed_customers_systems_collectors(cur, group_ids)
        seed_rules(cur, customer_ids)
        consultants_by_group = seed_contacts(cur, group_ids, customer_ids)
        seed_oncall(cur, consultants_by_group)
        seed_maintenance(cur, system_ids)
        seed_users(cur)
        # leaving the 'with' block commits the transaction

    print(f"Seeded {len(group_ids)} groups, {len(customer_ids)} customers, "
          f"{len(system_ids)} systems.")


if __name__ == "__main__":
    main()