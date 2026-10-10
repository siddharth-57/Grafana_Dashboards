"""Step 5: build alerts and notifications from incidents.json (logic in alerting.py)."""
import random
from collections import Counter
from datetime import datetime, timezone

import alerting as al
import generator as g
from db import get_connection

random.seed(11)
NOW = datetime.now(timezone.utc)


def main():
    incidents = sorted(g.load_incidents(), key=lambda x: x["start"])
    statuses = Counter()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE alerts, notifications RESTART IDENTITY")
        ctx = al.load_context(cur)
        for inc in incidents:
            if al.fired_time(inc, ctx) > NOW:
                continue                                   # not fired yet
            _, status = al.create_alert(cur, inc, ctx, NOW)
            statuses[status] += 1
        cur.execute("SELECT count(*) FROM notifications")
        n_notifications = cur.fetchone()[0]

    print(f"Alerts: {sum(statuses.values())}  {dict(statuses)}")
    print(f"Notifications: {n_notifications}")


if __name__ == "__main__":
    main()