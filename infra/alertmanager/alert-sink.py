"""받은 알림을 그대로 stdout 에 남기는 mock 리시버입니다.

PRD 의 제약이 "외부 연동 Mocking" 이라 Slack 을 안 붙인다. 그래도 증명할 것은 다 된다 —
알림이 뜨는 것, 그리고 server/data 가 **서로 다른 경로로 오는 것**.
경로는 URL 로 갈린다: /server · /data · /unrouted.

표준 라이브러리만 쓴다. 확인은 `docker compose logs alert-sink` 로 한다.
"""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = 5001


class Sink(BaseHTTPRequestHandler):

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        self.send_response(200)
        self.end_headers()

        channel = self.path.lstrip("/") or "unrouted"
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            print(f"[{channel}] 파싱 실패 {raw!r}", flush=True)
            return

        for alert in payload.get("alerts", []):
            labels = alert.get("labels", {})
            print(
                f"[{channel}] {alert.get('status')} "
                f"{labels.get('alertname')} "
                f"severity={labels.get('severity')} "
                f"channel={labels.get('channel', '(없음)')}",
                flush=True,
            )

    def log_message(self, *args):
        """접근 로그를 끈다 — 알림 줄만 남아야 눈으로 가를 수 있다."""


if __name__ == "__main__":
    print(f"alert-sink 시작 :{PORT}", flush=True)
    HTTPServer(("", PORT), Sink).serve_forever()
