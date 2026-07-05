from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path

PORT = int(os.environ.get("PORT", "8080"))
LOG_PATH = Path(os.environ.get("WEBHOOK_LOG", "/tmp/webhook-requests.log"))


class Handler(BaseHTTPRequestHandler):
    def _write_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._write_json(200, {"status": "ok"})
            return
        self._write_json(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8", errors="replace")
        entry = {
            "path": self.path,
            "method": "POST",
            "body": body,
            "headers": dict(self.headers),
        }
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
        print(json.dumps(entry), flush=True)
        self._write_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        return


def main():
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"webhook listening on :{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
