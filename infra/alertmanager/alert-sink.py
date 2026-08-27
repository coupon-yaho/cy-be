"""받은 알림을 stdout 에 남기고, 웹훅이 있으면 Slack 으로도 보냅니다.

경로는 URL 로 갈린다: /server · /data · /unrouted.
`server` 는 **배치·서버가 일을 안 한다**, `data` 는 **데이터가 틀렸다**(서버를 고쳐도
안 사라진다)이고, 이 저장소는 그 둘을 같은 알람으로 묶지 않는다.

<b>왜 Slack 을 붙였나.</b> PRD 제약이 "외부 연동 Mocking" 이라 stdout 만으로 뒀는데,
그러면 **아무도 안 본다.** 실측 — 이 파일을 고칠 때 이미 `critical` 하나를 포함해 셋이
발화 중이었고(`ExpireNeverSucceeded`·`ExpireMetricsUnknown`·`CleanupNeverSucceeded`)
`docker compose logs alert-sink` 를 쳐야만 보였다. `docs/13` §2 제목이 그것이다 —
*"성공했는데 맞게 했나 를 아무도 안 본다."*

<b>웹훅이 없으면 stdout 만 한다.</b> 없다고 죽지 않는다 — 그러면 리시버가 없어서
Alertmanager 가 실패하고, 그 실패를 알릴 경로도 같이 사라진다.

<b>URL 은 환경변수로만 받는다.</b> 저장소에 안 적는다. compose 가 `db.env` 처럼
추적 안 되는 파일에서 넘긴다. 이 컨테이너는 호스트 포트 매핑이 없고 `read_only`,
`cap_drop: [ALL]`, 비루트(65534)로 돈다.

표준 라이브러리만 쓴다. 확인은 `docker compose logs alert-sink` 로 한다.
"""

import json
import os
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 5001

# 없으면 stdout 만 한다. 있으면 Slack 으로도 보낸다.
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()

# **기동 때 한 번 거른다.** 값이 틀렸으면 첫 알림까지 기다리지 않고 지금 보이게 한다 —
# 그 알림이 critical 이면 그때가 제일 나쁜 순간이다. 흔한 실수 둘을 잡는다:
# 스킴을 흘리고 복사한 경우, compose 가 따옴표째 값으로 읽은 경우.
if SLACK_WEBHOOK_URL and not SLACK_WEBHOOK_URL.startswith("https://"):
    # 값은 안 싣는다 — 웹훅 URL 자체가 자격증명이다.
    print("[!] SLACK_WEBHOOK_URL 이 https:// 로 시작하지 않는다. stdout 만 한다",
          flush=True)
    SLACK_WEBHOOK_URL = ""

# Slack 이 느려도 알림 수신을 붙잡으면 안 된다. 이 리시버가 밀리면 그동안 들어온
# 알림이 통째로 밀린다 — 알림을 보여 주려고 만든 것이 알림을 못 받는 상태가 된다.
SLACK_TIMEOUT = 5

# **한 번만 다시 보낸다.** 우리는 이미 200 을 냈으므로 Alertmanager 가 재시도하지 않는다.
# 그러니 순간 실패는 여기서 흡수해야 한다 — 그렇게 안 하면 **repeat_interval(1시간)
# 안에 해소되는 알림은 영영 한 번도 안 간다**(resolved 는 안 보내므로).
# 두 번을 넘기지 않는 이유는 위 문단과 같다. 최악 지연이 5+1+5=11초로 유한해야 한다
# (마지막 실패 뒤에는 안 기다린다 — 안 그러면 12초다).
SLACK_RETRIES = 1
SLACK_RETRY_DELAY = 1

# 발화만 보낸다. resolved 까지 보내면 한 사건이 두 줄이 되고, 이 저장소의 알림 수에서는
# 그게 곧 아무도 안 읽는 채널이 된다.
# (**개수를 여기 적지 않는다** — 대장은 docs/14 의 채널 표이고 AlertChannelRegistryTest 가
#  대조한다. alertmanager.yml 이 이미 못 박아 둔 규칙인데 한때 여기서 다시 깼다.)
_SEVERITY_MARK = {"critical": "\U0001F534", "warning": "\U0001F7E0", "info": "\U0001F535"}


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
            self._to_slack(channel, alert)

    def _to_slack(self, channel, alert):
        """<b>보내다 실패해도 이 요청을 죽이지 않는다.</b>

        여기서 예외가 나가면 200 을 이미 보낸 뒤라 Alertmanager 는 성공으로 알고,
        같은 payload 의 **뒤 알림들이 통째로 유실된다.** 그래서 전부 잡고 로그만 남긴다.
        """
        if not SLACK_WEBHOOK_URL or alert.get("status") != "firing":
            return

        labels = alert.get("labels", {})
        ann = alert.get("annotations", {})
        sev = labels.get("severity", "unknown")
        mark = _SEVERITY_MARK.get(sev, "\u26AA")
        text = (
            f"{mark} *{sev}* · `{channel}` — *{labels.get('alertname', '(이름 없음)')}*\n"
            f"{ann.get('summary', '')}"
        )
        desc = ann.get("description")
        if desc:
            text += f"\n{desc}"

        for attempt in range(SLACK_RETRIES + 1):
            last = attempt == SLACK_RETRIES
            if self._post_to_slack(text, last=last):
                return
            # **마지막 실패 뒤에는 안 기다린다.** 기다려도 다시 시도할 것이 없고,
            # 그 1초가 같은 payload 의 **뒤 알림들을 건마다 밀어낸다.**
            if not last:
                time.sleep(SLACK_RETRY_DELAY)

    def _post_to_slack(self, text, last):
        """보냈으면 {@code True}. 실패는 마지막 시도에서만 로그를 남긴다."""
        try:
            body = json.dumps({"text": text}).encode()
            req = urllib.request.Request(
                SLACK_WEBHOOK_URL, data=body,
                headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=SLACK_TIMEOUT) as r:
                if r.status == 200:
                    return True
                # 여기 오는 것은 2xx·3xx 중 200 이 아닌 것뿐이다 — 4xx·5xx 는 아래
                # HTTPError 로 빠진다.
                if last:
                    print(f"[!] Slack 이 {r.status} 를 냈다", flush=True)
                return False
        except urllib.error.HTTPError as e:
            # **상태 코드를 남긴다.** urlopen 은 4xx·5xx 를 HTTPError 로 **던지므로**
            # 위 r.status 분기에 도달하지 않는다. 타입 이름만 남기면
            # **403(웹훅이 폐기됐다)과 500(Slack 장애)이 같은 줄이 된다** — 전자는
            # 사람이 웹훅을 다시 발급해야 하고 후자는 기다리면 된다.
            # `e.code` 에는 URL 이 안 들어간다(실측: str(e) 가 "HTTP Error 403: Forbidden").
            if last:
                print(f"[!] Slack 이 {e.code} 를 냈다", flush=True)
            return False
        except Exception as e:  # noqa: BLE001 — 아래 이유로 전부 잡는다
            # **예외 종류를 좁히면 안 된다.** 한때 (URLError, OSError, TimeoutError) 였는데
            # `Request()` 생성이 try 밖이었고, 스킴 없는 URL 에 `ValueError` 를 던진다 —
            # 그 메시지에 **URL 전문이 들어간다.** 재현: 알림 3건짜리 payload 에서 첫 건만
            # stdout 에 남고 뒤 둘은 사라졌으며, 웹훅 URL 이 트레이스백으로 찍혔다.
            # `http.client.InvalidURL`(제어문자)·`AttributeError`(labels 가 null)도 같은 자리다.
            #
            # **예외 객체를 포맷하지 않는다.** 타입 이름만 남긴다 — 메시지에 URL 이 들어온다.
            if last:
                print(f"[!] Slack 전송 실패: {type(e).__name__}", flush=True)
            return False

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
    print(f"alert-sink 시작 :{PORT} "
          f"slack={'연결됨' if SLACK_WEBHOOK_URL else '없음(stdout 만)'}", flush=True)
    # ThreadingHTTPServer 다. HTTPServer 는 요청을 **순차 처리**해서, 한 연결이 붙잡히면
    # 그동안 들어온 알림이 전부 밀린다 — 알림이 뜨는 것을 보여 주려고 만든 리시버가
    # 알림을 못 받는 상태가 된다. 데드라인이 그 시간을 유한하게 만들지만, 그 동안에도
    # 다른 알림은 받아야 한다.
    ThreadingHTTPServer(("", PORT), Sink).serve_forever()
