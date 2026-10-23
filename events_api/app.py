import json
import random
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

USERS = [f"user_{i}" for i in range(1, 11)]
PAGES = ["/home", "/pricing", "/blog", "/docs", "/contact"]

random.seed(42)
NOW = datetime.now()
EVENTS = []
for _ in range(3000):
    ts = NOW - timedelta(seconds=random.randint(0, 30 * 24 * 3600))
    EVENTS.append({
        "user": random.choice(USERS),
        "date": ts.strftime("%Y-%m-%d"),
        "timestamp": ts.isoformat(),
        "page": random.choice(PAGES),
    })
EVENTS.sort(key=lambda e: e["timestamp"])


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        if url.path != "/events":
            self.send_error(404)
            return
        q = parse_qs(url.query)
        start = q.get("start_date", [None])[0]  # used later in the chapter
        end = q.get("end_date", [None])[0]
        data = [
            e for e in EVENTS
            if (not start or e["date"] >= start) and (not end or e["date"] < end)
        ]
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)


HTTPServer(("0.0.0.0", 5000), Handler).serve_forever()