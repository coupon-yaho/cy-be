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

    # 느린 바디 전송을 끊는다. 상한만으로는 한 연결이 오래 붙잡는 것을 못 막는다.
    timeout = 5

    # 바디 상한. alertmanager 가 보내는 payload 는 수 KB 라 넉넉하다.
    # 상한이 없으면 Content-Length 가 말하는 만큼 통째로 메모리에 올린다.
    MAX_BODY = 1 << 20

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
        except ValueError:
            # 숫자가 아니면 예외가 요청 하나를 죽이고 트레이스백만 남는다 —
            # 알림 줄만 보이게 하려고 만든 로그가 그걸로 덮인다.
            self.send_response(400)
            self.end_headers()
            print("[!] Content-Length 가 숫자가 아니다", flush=True)
            return

        if length > self.MAX_BODY:
            self.send_response(413)
            self.end_headers()
            print(f"[!] 바디가 상한을 넘었다 ({length} > {self.MAX_BODY})", flush=True)
            return

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
