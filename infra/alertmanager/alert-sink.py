"""받은 알림을 그대로 stdout 에 남기는 mock 리시버입니다.

PRD 의 제약이 "외부 연동 Mocking" 이라 Slack 을 안 붙인다. 그래도 증명할 것은 다 된다 —
알림이 뜨는 것, 그리고 server/data 가 **서로 다른 경로로 오는 것**.
경로는 URL 로 갈린다: /server · /data · /unrouted.

표준 라이브러리만 쓴다. 확인은 `docker compose logs alert-sink` 로 한다.
"""

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 5001


class Sink(BaseHTTPRequestHandler):

    # 소켓 타임아웃. 이것만으로는 부족하다 — settimeout 은 **매 recv 호출** 단위라,
    # 상대가 4.9초마다 1바이트씩 보내면 매번 타임아웃 전에 성공해서 전체 읽기가 안 끊긴다.
    # 전송 전체의 데드라인은 BODY_DEADLINE 이 따로 진다.
    timeout = 5

    # 바디 전체를 받는 데 허용하는 총 시간. 얼마를 보내든 이 안에 끝나야 한다.
    BODY_DEADLINE = 10

    # 한 번에 읽는 크기. 통째로 read(length) 하면 그 호출 안에서 시간을 못 잰다.
    CHUNK = 8192

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

        if length < 0:
            # 음수는 상한 검사(length > MAX_BODY)를 그대로 통과한다. 그리고 read(-1) 은
            # EOF 까지 읽으므로, 상대가 연결을 안 끊으면 이 요청이 오래 붙잡힌다.
            self.send_response(400)
            self.end_headers()
            print("[!] Content-Length 가 음수다", flush=True)
            return

        if length > self.MAX_BODY:
            self.send_response(413)
            self.end_headers()
            print(f"[!] 바디가 상한을 넘었다 ({length} > {self.MAX_BODY})", flush=True)
            return

        raw = self._read_body(length)
        if raw is None:
            self.send_response(408)
            self.end_headers()
            print(f"[!] 바디 전송이 {self.BODY_DEADLINE}초를 넘겼다", flush=True)
            return

        self.send_response(200)
        self.end_headers()

        channel = self.path.lstrip("/") or "unrouted"
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            # 원문을 통째로 싣지 않는다. 상한이 1MB 라 그것이 전부 로그로 나가면
            # 알림 줄만 눈으로 가르려고 만든 이 로그가 그 한 줄에 덮인다.
            print(f"[{channel}] 파싱 실패 ({len(raw)}바이트) {raw[:512]!r}", flush=True)
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

    def _read_body(self, length):
        """<b>느리게 흘려보내는 전송을 끊는다.</b>

        ``read(length)`` 한 번으로 받으면 그 호출 안에서는 시간을 못 잰다. 소켓 타임아웃은
        recv 하나가 얼마나 기다렸는지만 보므로, 그 아래로 잘게 나눠 보내면 영원히 안 끊긴다.
        그래서 청크로 읽으면서 **누적 시간**을 직접 본다. 데드라인을 넘기면 ``None``.
        """
        deadline = time.monotonic() + self.BODY_DEADLINE
        chunks = []
        remaining = length
        while remaining > 0:
            if time.monotonic() > deadline:
                return None
            chunk = self.rfile.read(min(self.CHUNK, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def log_message(self, *args):
        """접근 로그를 끈다 — 알림 줄만 남아야 눈으로 가를 수 있다."""


if __name__ == "__main__":
    print(f"alert-sink 시작 :{PORT}", flush=True)
    # ThreadingHTTPServer 다. HTTPServer 는 요청을 **순차 처리**해서, 한 연결이 붙잡히면
    # 그동안 들어온 알림이 전부 밀린다 — 알림이 뜨는 것을 보여 주려고 만든 리시버가
    # 알림을 못 받는 상태가 된다. 데드라인이 그 시간을 유한하게 만들지만, 그 동안에도
    # 다른 알림은 받아야 한다.
    ThreadingHTTPServer(("", PORT), Sink).serve_forever()
