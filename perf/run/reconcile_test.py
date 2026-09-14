#!/usr/bin/env python3
"""reconcile.py 의 판정을 픽스처 파일로 태운다 (CY-960).

    python3 perf/run/reconcile_test.py

**왜 파일 픽스처인가** — 판정을 파이썬에 두고 DB 읽기를 셸에 둔 이유가 이것이다.
재기동도, 늦은 취소 경합도, 조회 절반 실패도 DB 없이 **그 형상 그대로** 만들 수 있다.
DB 를 띄워야만 태울 수 있는 시험은 아무도 안 돌린다.

**대칭을 피한다.** 회차·회원·발급 번호를 서로 다른 값으로 준다. 셋이 같으면 두 축을
맞바꾼 구현도 통과한다.
"""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLI = HERE / "reconcile.py"

# 순위 계산처럼 프로세스를 띄울 필요가 없는 것은 직접 부른다.
sys.path.insert(0, str(HERE))
import reconcile as reconcile_mod  # noqa: E402

ROUND = 7001          # 회차
MEMBER = 4200         # 회원 — 회차와 자릿수까지 다르게 둔다
ISSUANCE = 990001     # 발급 번호
KEY = "11111111-2222-4333-8444-555555555555"


def as_report(when, mark):
    """진짜 대조 보고서의 모양. **픽스처가 이 모양이어야 한다** — 아니면 판정 불가로
    걸러지는 것이 정상인 파일을 "보고서" 로 놓고 시험하게 된다."""
    return {"schema": reconcile_mod.SCHEMA, "generated_at": when,
            "counts": {}, "unjudged": [], "mark": mark}


GRADE = "VIP"          # 요청 내용. 서버가 canonicalRequest 로 해시하는 셋째 값이다


def issuance(iid, coupon, member, status, content=GRADE):
    """발급 한 행. **덤프는 다섯 칸이다** — 내용(issued_grade)이 마지막이다."""
    return (iid, coupon, member, status, content)


def tsv(rows):
    body = "".join("\t".join(str(c) for c in r) + "\n" for r in rows)
    return body + f"#EOF\t{len(rows)}\n"


def derive_histories(records, issuances):
    """발급마다 ISSUE 이력 한 줄. **기본값이 런타임에 가능한 형상이어야 한다.**

    실제로 한 발급은 어떤 접수 키의 요청이 만든 것이다. 그 대상으로 보낸 REQ 가 있으면
    그 키가 만든 것으로 보고, 없으면 우리가 모르는 키가 만든 것으로 본다 — 그것이
    고아의 실제 모습이다. 키 없는 발급을 기본값으로 두면 런타임에 없는 형상이 된다.

    다른 키가 만든 발급을 시험하려면 `histories=` 로 직접 준다.
    """
    by_target = {}
    for line in records:
        parts = line.split("\t")
        if len(parts) in (5, 6) and parts[0] == "CY960" and parts[1] == "REQ":
            by_target[(parts[3], parts[4])] = parts[2]
    return [(iid, by_target.get((str(cid), str(mid)), foreign_key(iid)))
            for iid, cid, mid, _status, _content in issuances]


# 알림 id 는 발급 id 와 **겹치지 않게** 띄운다. 같은 수를 쓰면 둘을 맞바꾼
# 코드가 시험을 그대로 통과한다 — 생성자 인자 순서를 착각해 값이 맞아 보였던
# 일이 이미 한 번 있었다.
NOTIFICATION_ID_BASE = 500000


def derive_notifications(issuances):
    """발급마다 알림 하나 + `(1, INITIAL, PENDING)` 아웃박스 하나.

    **기본값이 런타임에 가능한 형상이어야 한다.** request() 가 발급 트랜잭션
    안에서 딱 그 둘을 넣으므로, 알림 없는 발급을 기본값으로 두면 멀쩡한 회차가
    전부 NOTIFICATION_MISSING 으로 빨개진다.

    상태를 둘 다 PENDING 으로 두는 것은 request() 가 그렇게 넣기 때문이다(코드로
    확인). 릴레이가 돌면 SENT·PUBLISHED 로 바뀌는데, **판정 넷 중 어느 것도 상태를
    안 읽으므로 답은 같다** — 그 경우도 시험 하나로 태운다.
    """
    return [(iid + NOTIFICATION_ID_BASE, iid, cid, mid, "PENDING", 1, "INITIAL", "PENDING")
            for iid, cid, mid, _status, _content in issuances]


def foreign_key(issuance_id):
    """우리가 보낸 적 없는 접수 키. UUID 모양이어야 런타임에 가능한 값이다."""
    return f"00000000-0000-4000-8000-{issuance_id:012d}"


class Fixture:
    """한 반복 디렉터리를 만든다. 안 준 파일은 안 만든다 — 그것이 조회 실패의 형상이다."""

    def __init__(self, tmp, records, issuances=(), idem=(), *,
                 configured=1, dropped=0, round_id=ROUND,
                 write_issuances=True, write_idem=True, marker=True,
                 measure_attempts=None, measure_retries=None,
                 histories=None, write_histories=True,
                 notifications=None, write_notifications=True):
        self.dir = Path(tmp)
        (self.dir / "round.json").write_text(json.dumps({
            "engine": "V2", "target_round_id": round_id,
            "configured_requests": configured}))
        # measure_attempts 는 k6 가 늘 낸다. 픽스처가 빼면 런타임에 없는 형상이 된다 —
        # 기본은 이 파일의 REQ 줄 수와 맞춘다.
        # k6 는 **이터레이션**을 시도로 세고, 같은 키의 재전송은 시도가 아니라
        # 재전송으로 센다. 픽스처가 REQ 줄 수를 시도로 쓰면 재전송이 있는 회차에서
        # 런타임에 없는 모양이 된다 — 실제로 그렇게 썼다가 시험 셋이 빨개졌다.
        mine = [r.split("\t") for r in records
                if r.startswith("CY960\tREQ\t") and r.split("\t")[3:4] == [str(round_id)]]
        attempts = measure_attempts if measure_attempts is not None else len(
            {r[2] for r in mine})
        retries = measure_retries if measure_retries is not None else (
            len(mine) - len({r[2] for r in mine}))
        (self.dir / "k6-summary.json").write_text(json.dumps({
            "metrics": {"dropped_iterations": {"values": {"count": dropped}}},
            "perf": {"measure_attempts": attempts, "measure_retries": retries}}))
        # 기록이 켜진 회차의 파일에는 setup() 이 낸 표식이 **반드시** 앞에 있다.
        # 픽스처가 그것을 빼면 런타임에 없는 형상을 시험하게 된다.
        head = [f"CY960\tRUN\t{round_id}"] if marker else []
        (self.dir / "requests.log").write_text(
            "".join(l + "\n" for l in head + list(records)))
        if write_issuances:
            (self.dir / "db-issuances.tsv").write_text(tsv(issuances))
        if write_idem:
            (self.dir / "db-idempotency.tsv").write_text(tsv(idem))
        if write_histories:
            (self.dir / "db-histories.tsv").write_text(
                tsv(derive_histories(records, issuances)
                    if histories is None else histories))
        if write_notifications:
            (self.dir / "db-notifications.tsv").write_text(
                tsv(derive_notifications(issuances)
                    if notifications is None else notifications))

    def run(self):
        out = self.dir / "report.json"
        proc = subprocess.run(
            [sys.executable, str(CLI), str(self.dir), "--out", str(out)],
            capture_output=True, text=True)
        return proc.returncode, json.loads(out.read_text()), proc


class ReconcileTest(unittest.TestCase):

    def check(self, records, issuances=(), idem=(), **kw):
        with tempfile.TemporaryDirectory() as tmp:
            return Fixture(tmp, records, issuances, idem, **kw).run()

    def types(self, report):
        return {t: n for t, n in report["counts"].items() if n}

    # ── 정상 ──────────────────────────────────────────────────────────

    def test_성공_응답과_발급이_맞으면_정상이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(code, 0)

    def test_명시적_거절은_발급이_없어야_정상이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-306"])
        self.assertEqual(self.types(report), {"MATCHED_REJECTED": 1})
        self.assertEqual(code, 0)

    # ── 응답 유실 ────────────────────────────────────────────────────

    def test_응답을_잃어도_발급이_있으면_해소된다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tUNKNOWN\t{KEY}\t1050"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"RESOLVED_ISSUED": 1})
        self.assertEqual(code, 0)

    def test_응답_줄이_아예_없어도_같은_판정이_난다(self):
        # k6 가 중간에 죽으면 REQ 만 남는다. 그것도 결과 불명이다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"RESOLVED_ISSUED": 1})
        self.assertEqual(code, 0)

    def test_성공을_받았는데_발급이_없으면_유실이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"])
        self.assertEqual(self.types(report), {"LOST": 1})
        self.assertEqual(code, 1)

    # ── 양쪽 동시 누락 ───────────────────────────────────────────────

    def test_양쪽에서_동시에_빠지면_미해결로_남는다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}"])
        self.assertEqual(self.types(report), {"UNRESOLVED": 1})
        self.assertEqual(code, 4)

    def test_보낸_기록이_없으면_그_건은_보이지도_않는다(self):
        # 위 시험과 **DB 상태가 똑같다.** 다른 것은 REQ 한 줄뿐이고, 그 한 줄이
        # 있고 없고가 "미해결 1건" 과 "아무 일 없음" 을 가른다.
        # 요청 **전에** 기록하는 설계가 무엇을 사는지가 이 두 시험의 차이다.
        code, report, _ = self.check([])
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 0)

    def test_재전송이_있어도_설명_안_되는_건이_음수가_안_된다(self):
        # **설정 요청 수가 세는 것은 신청이지 보낸 횟수가 아니다.** 줄 수와 맞대면
        # 재전송만큼 음수가 나온다 — 실제 k6 출력으로 돌려 보다 잡았다(설정 11 ·
        # 줄 16 → "설명 안 되는 -5건").
        _, report, proc = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tUNKNOWN\t{KEY}\t1050",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")], configured=1)
        self.assertEqual(report["totals"]["recorded_applications"], 1)
        self.assertEqual(report["totals"]["recorded_requests"], 2)
        # **두 문구가 모두 안 나와야 한다.** 부호만 갈라 놓고 줄 수와 맞대면
        # "설정보다 신청이 많다" 로 바뀔 뿐 여전히 거짓이다 — 신청은 하나다.
        self.assertNotIn("설명 안 되는", proc.stdout)
        self.assertNotIn("설정보다 신청이", proc.stdout)

    def test_설정보다_신청이_많으면_거꾸로_적지_않는다(self):
        # 재전송이 새 키를 쓰면 신청이 설정보다 많아진다. 그때 "보낸 기록이 없다" 고
        # 적으면 **문구가 사실과 거꾸로다** — 기록은 오히려 더 많다. 실측으로 -1 을 봤다.
        other = "99999999-8888-4777-8666-555555555555"
        _, report, proc = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tUNKNOWN\t{KEY}\t1050",
             f"CY960\tREQ\t{other}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{other}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            configured=1, measure_attempts=1, measure_retries=1)
        self.assertIn("설정보다 신청이 1건 많다", proc.stdout)
        self.assertNotIn("설명 안 되는", proc.stdout)
        self.assertNotIn("-1건", proc.stdout)

    def test_기록도_못쏨도_아닌_요청은_총계로_드러난다(self):
        _, report, proc = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")], configured=5, dropped=2)
        # 설정 5 − 신청 1 − 못 쏨 2 = 2건이 어디에도 없다.
        self.assertIn("설명 안 되는 2건", proc.stdout)

    def test_k6_가_센_시도와_기록_줄_수가_어긋나면_판정하지_않는다(self):
        # 둘 다 measure() 한 번에 하나씩 는다. 어긋나면 기록이 온전하지 않다는 뜻이고,
        # **그 기록으로 낸 판정은 못 믿는다** — 빠진 키는 애초에 없던 것처럼 보인다.
        code, report, proc = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")], measure_attempts=4)
        self.assertEqual(report["totals"]["k6_measure_attempts"], 4)
        self.assertEqual(report["completeness"]["records"], "PARTIAL")
        self.assertTrue(any("온전하지 않다" in p for p in report["problems"]),
                        report["problems"])
        self.assertIn("온전하지 않다", proc.stderr)
        self.assertEqual(code, 3)

    def test_재전송이_있어도_기록이_온전하면_판정한다(self):
        # 같은 키로 두 번 보낸 회차다. 예전 검사(줄 수 == 시도)는 이걸 "기록이
        # 짧다/길다" 로 오탐했다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tUNKNOWN\t{KEY}\t1050",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(report["completeness"]["records"], "COMPLETE")
        self.assertEqual(report["totals"]["k6_measure_retries"], 1)
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(code, 0)

    def test_재전송이_새_키를_쓰면_잡는다(self):
        """**줄 수 검사로는 못 잡는 자리다.**

        재전송은 같은 접수 키여야 한다. 새 키를 쓰면 서버가 그것을 새 신청으로 보아
        한 사람이 둘을 받는다 — 그런데 `줄 == 시도 + 재전송` 은 그대로 맞는다.
        """
        other = "99999999-8888-4777-8666-555555555555"
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tUNKNOWN\t{KEY}\t1050",
             f"CY960\tREQ\t{other}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{other}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            measure_attempts=1, measure_retries=1)   # k6 는 한 신청·한 재전송으로 셌다
        self.assertEqual(report["completeness"]["records"], "PARTIAL")
        self.assertTrue(any("같은 키를 안 썼을 수 있다" in p for p in report["problems"]),
                        report["problems"])
        self.assertEqual(code, 3)

    def test_재전송_줄이_유실되면_잡는다(self):
        # 키 수는 맞는데 줄이 하나 모자라다. 키 검사로는 못 잡는다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            measure_attempts=1, measure_retries=1)
        self.assertEqual(report["completeness"]["records"], "PARTIAL")
        self.assertTrue(any("기록이 온전하지 않다" in p for p in report["problems"]),
                        report["problems"])
        self.assertEqual(code, 3)

    def test_결과_줄이_깨지면_판정하지_않는다(self):
        # **시도 수로는 안 잡히는 자리다.** REQ 는 멀쩡하니 k6 가 센 수와 똑같고,
        # 깨진 것은 결과 줄 하나뿐이다. 그대로 두면 성공을 받은 건이 결과 불명이 되어
        # 유실(결함, 종료 1)이 미해결(보류, 종료 4)로 내려간다 — 실측으로 확인했다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}"])
        self.assertEqual(report["totals"]["malformed_lines"], 1)
        self.assertEqual(report["completeness"]["records"], "PARTIAL")
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 3)

    def test_남의_줄은_형식_깨짐이_아니라서_판정을_안_막는다(self):
        # 접두사가 없는 줄은 우리 기록이 아니다. 그것까지 막으면 k6 가 낸 아무 줄
        # 하나에 대조가 통째로 멈춘다.
        code, report, _ = self.check(
            ["k6 가 낸 아무 줄", f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(report["completeness"]["records"], "COMPLETE")
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(code, 0)

    def test_기록이_짧으면_있던_결함도_판정하지_않는다(self):
        # 짧은 기록으로 낸 유실 수는 실제보다 작다. 그 수를 내놓으면 보는 사람이
        # 그것을 전부로 읽는다 — 안 세는 편이 낫다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            measure_attempts=9)
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 3)

    def test_시도와_기록이_맞으면_아무_말도_안_한다(self):
        _, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(report["problems"], [])

    # ── 고아 · 중복 ──────────────────────────────────────────────────

    def test_보낸_적_없는_발급은_고아다(self):
        code, report, _ = self.check([], [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"ORPHAN": 1})
        self.assertEqual(code, 1)

    def test_다른_회차의_발급은_고아가_아니다(self):
        # 워밍업 회차의 발급까지 고아로 세면 정상 회차가 전량 결함으로 보인다.
        code, report, _ = self.check([], [issuance(ISSUANCE, ROUND + 1, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 0)

    def test_한_대상에_발급이_둘이면_중복이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED"),
             issuance(ISSUANCE + 1, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"DUPLICATE": 1})
        self.assertEqual(code, 1)

    # ── 재전송을 한 신청으로 묶는다 ──────────────────────────────────

    def test_같은_접수_키의_재전송은_한_신청이다(self):
        # 첫 시도는 응답을 잃었고 두 번째는 "이미 있다" 로 거절됐는데 세 번째가 성공했다.
        # 세 줄이지만 신청은 하나고, 결론은 가장 확정적인 것 — 성공이다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tUNKNOWN\t{KEY}\t1050",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tREJECTED\t{KEY}\tCOUPON-305",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(report["findings"][0]["attempts"], 3)
        self.assertEqual(code, 0)

    def test_한_접수_키에_예약번호가_둘이면_불일치다(self):
        # 멱등이 깨졌다는 뜻이다. 재전송이 새 발급을 만들었다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE + 9}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MISMATCH": 1})
        self.assertEqual(code, 1)

    def test_받은_예약번호가_DB_와_다르면_불일치다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE + 5, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MISMATCH": 1})
        self.assertEqual(code, 1)

    # ── 접수 키를 축으로 삼아야만 보이는 것 (CY-962) ─────────────────

    def test_대상은_맞는데_내_키의_발급이_아니면_잡는다(self):
        # **대상으로 조인하면 이것이 MATCHED 로 보인다.** 회원도 회차도 맞으니까.
        # 내 신청이 그 발급이 됐는지는 접수 키만이 답한다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, foreign_key(ISSUANCE))])
        self.assertEqual(self.types(report), {"KEY_MISMATCH": 1, "ORPHAN": 1})
        self.assertEqual(code, 1)

    def test_내_키의_발급이_다른_대상_앞으로_있으면_잡는다(self):
        # **대상으로 조인하면 낼 수 없는 판정이다** — 대상으로 찾았으니 찾힌 행의
        # 대상은 언제나 맞다. 예전에는 한 건의 대상 오류가 유실 하나와 고아 하나로
        # 흩어졌고, 둘을 이으려면 사람이 손으로 맞춰야 했다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER + 7, "ISSUED")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(self.types(report), {"TARGET_MISMATCH": 1})
        self.assertEqual(report["findings"][0]["key"], KEY)
        self.assertIn(f"회원 {MEMBER + 7}", report["findings"][0]["detail"])
        self.assertEqual(code, 1)

    def test_내_키의_발급이_다른_회차_앞으로_있어도_잡는다(self):
        # 덤프를 대상 회차로만 뜨면 이 행이 아예 없어 LOST 로 읽힌다 — 결함은 잡되
        # 이름이 틀린다. reconcile.sh 가 성공 응답의 예약번호로 회차 밖을 짚는다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND + 5, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(self.types(report), {"TARGET_MISMATCH": 1})
        self.assertIn(f"회차 {ROUND + 5}", report["findings"][0]["detail"])
        self.assertEqual(code, 1)

    # ── 요청 내용 축 (CY-970) ────────────────────────────────────────

    def test_대상은_맞는데_내용이_다르면_잡는다(self):
        """요구사항 §6.3 이 *"접수 키·대상·내용"* 을 셋으로 적는 이유다.

        서버가 보는 요청 내용은 `canonicalRequest` 의 셋이고 그 셋째가 여기 담긴다.
        대상만 맞대면 **엉뚱한 내용으로 발급된 건이 일치로 읽힌다.**
        """
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED", content="BASIC")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(self.types(report), {"CONTENT_MISMATCH": 1})
        self.assertIn("BASIC", report["findings"][0]["detail"])
        self.assertEqual(code, 1)

    def test_응답을_잃어도_내용이_다르면_잡는다(self):
        """**`OK` 가 없는 순수 `UNKNOWN` 이다.**

        예전에는 대상·내용 검사가 `OK` 갈래 안에만 있어서, 응답을 잃은 건은 내용이
        달라도 `RESOLVED_ISSUED`(정상, 종료 0)로 나갔다 — 실측으로 재현했다.
        내 키의 발급이 내가 요청한 내용인지는 **응답을 받았든 잃었든 같은 질문**이다.
        """
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tUNKNOWN\t{KEY}\t1050"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED", content="BASIC")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(self.types(report), {"CONTENT_MISMATCH": 1})
        self.assertEqual(code, 1)

    def test_응답을_잃어도_대상이_다르면_잡는다(self):
        # 같은 구멍이 대상 축에도 있었다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tUNKNOWN\t{KEY}\t1050"],
            [issuance(ISSUANCE, ROUND, MEMBER + 7, "ISSUED")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(self.types(report), {"TARGET_MISMATCH": 1})
        self.assertEqual(code, 1)

    def test_응답을_잃었고_내용도_맞으면_해소다(self):
        # 고친 뒤에도 정상 경로가 살아 있는지 본다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tUNKNOWN\t{KEY}\t1050"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(self.types(report), {"RESOLVED_ISSUED": 1})
        self.assertEqual(code, 0)

    def test_내용을_모르면_해소를_삼키지_않는다(self):
        # 옛 형식 기록 + 응답 유실. CONTENT_MISMATCH 가 unjudged 라 continue 가
        # 기록을 통째로 삼키면 안 된다 — RESOLVED_ISSUED 로 가야 한다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",
             f"CY960\tUNKNOWN\t{KEY}\t1050"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED", content="BASIC")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(self.types(report), {"RESOLVED_ISSUED": 1})
        self.assertIn("CONTENT_MISMATCH", report["unjudged"])
        self.assertEqual(code, 3)

    def test_내용이_같으면_일치다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(code, 0)

    def test_내용을_안_담은_옛_기록은_그_축을_판정하지_않는다(self):
        # **거절하지 않는다** — 옛 회차를 다시 대조하는 것이 이 도구의 쓰임새다.
        # 대신 없는 값을 "맞다" 로 세지도 않는다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",          # 5칸 = 옛 형식
             f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED", content="BASIC")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(report["completeness"]["content"], "MISSING")
        self.assertIn("CONTENT_MISMATCH", report["unjudged"])
        self.assertEqual(report["totals"]["malformed_lines"], 0)   # 형식 깨짐이 아니다
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(code, 3)                                   # 일부 판정 불가

    def test_한_키에_내용이_둘이면_불일치다(self):
        # 같은 키로 다른 내용을 보낸 것이다 — 서버는 COUPON-404 로 거절한다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tUNKNOWN\t{KEY}\t1050",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\tBASIC",
             f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(self.types(report), {"CONTENT_MISMATCH": 1})
        self.assertEqual(code, 1)

    def test_내용_불일치는_예약번호_불일치와_다른_유형이다(self):
        # 원인이 다르다 — 이쪽은 요청↔저장, 저쪽은 응답↔저장이다.
        _, content_bad, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED", content="BASIC")],
            histories=[(ISSUANCE, KEY)])
        _, id_bad, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tOK\t{KEY}\t{ISSUANCE + 5}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, KEY)])
        self.assertEqual(self.types(content_bad), {"CONTENT_MISMATCH": 1})
        self.assertEqual(self.types(id_bad), {"MISMATCH": 1})

    def test_한_접수_키가_발급_둘을_만들면_멱등이_깨진_것이다(self):
        # 대상이 서로 달라도 잡힌다 — 한 신청이 두 발급을 만든 것이 결함이다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED"),
             issuance(ISSUANCE + 1, ROUND, MEMBER + 3, "ISSUED")],
            histories=[(ISSUANCE, KEY), (ISSUANCE + 1, KEY)])
        self.assertEqual(self.types(report), {"DUPLICATE": 1})
        self.assertIn("멱등이 깨졌다", report["findings"][0]["detail"])
        self.assertEqual(code, 1)

    def test_한_대상의_중복을_두_번_세지_않는다(self):
        # 같은 키가 만든 두 발급이 같은 대상에 있으면 DUPLICATE 하나로 족하다.
        # DUPLICATE_TARGET 까지 내면 한 상황이 결함 둘로 읽힌다.
        _, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED"),
             issuance(ISSUANCE + 1, ROUND, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, KEY), (ISSUANCE + 1, KEY)])
        self.assertEqual(self.types(report), {"DUPLICATE": 1})

    def test_서로_다른_키가_한_대상에_발급을_만들면_그건_따로_센다(self):
        # 이쪽은 uk_coupon_member 가 깨진 것이라 DUPLICATE 와 다른 사실이다.
        other = foreign_key(ISSUANCE + 1)
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED"),
             issuance(ISSUANCE + 1, ROUND, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, KEY), (ISSUANCE + 1, other)])
        self.assertEqual(self.types(report),
                         {"MATCHED": 1, "DUPLICATE_TARGET": 1, "ORPHAN": 1})
        self.assertEqual(code, 1)

    def test_이력이_없는_발급은_어느_키에도_못_붙인다(self):
        # 시드 더미가 이렇게 생겼다 — issuances 에만 INSERT 하고 이력은 안 만든다.
        # 대상 회차 안에서 나오면 그 자체가 검출이다.
        code, report, _ = self.check(
            [], [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")], histories=[])
        self.assertEqual(self.types(report), {"UNATTRIBUTABLE": 1})
        self.assertIn("ISSUE 이력이 없다", report["findings"][0]["detail"])
        self.assertEqual(code, 1)

    def test_이력의_키가_갈리는_발급도_못_붙인다(self):
        code, report, _ = self.check(
            [], [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, KEY), (ISSUANCE, foreign_key(ISSUANCE))])
        self.assertEqual(self.types(report), {"UNATTRIBUTABLE": 1})
        self.assertIn("2가지다", report["findings"][0]["detail"])

    def test_키가_갈리는_이력은_어느_키에도_붙지_않는다(self):
        # 둘 중 하나를 골라 쓰면 그 발급이 누군가의 신청으로 **잘못 귀속된다.**
        # 고르는 규칙이 무엇이든(먼저·나중·작은 값) 틀리므로, 내 키가 앞서는 경우와
        # 뒤서는 경우를 **둘 다** 태운다 — 한쪽만 태우면 반대 규칙이 살아남는다.
        for other in ("00000000-0000-4000-8000-000000000001",
                      "ffffffff-ffff-4fff-8fff-ffffffffffff"):
            with self.subTest(other=other):
                code, report, _ = self.check(
                    [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
                     f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
                    [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
                    histories=[(ISSUANCE, KEY), (ISSUANCE, other)])
                self.assertEqual(self.types(report),
                                 {"KEY_MISMATCH": 1, "UNATTRIBUTABLE": 1})
                self.assertEqual(code, 1)

    def test_이력_조회가_실패하면_키로_가르는_판정을_안_한다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")], write_histories=False)
        self.assertEqual(report["completeness"]["histories"], "PARTIAL")
        self.assertIn("KEY_MISMATCH", report["unjudged"])
        self.assertIn("ORPHAN", report["unjudged"])
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 3)

    def test_고아의_이름은_그_발급이_단_접수_키다(self):
        # 대상으로 이름 붙이면 같은 대상의 두 고아가 전후 비교에서 한 원소로 뭉개진다.
        a, b = foreign_key(ISSUANCE), foreign_key(ISSUANCE + 1)
        _, report, _ = self.check(
            [], [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED"),
                 issuance(ISSUANCE + 1, ROUND, MEMBER + 1, "ISSUED")],
            histories=[(ISSUANCE, a), (ISSUANCE + 1, b)])
        self.assertEqual(sorted(f["key"] for f in report["findings"]), sorted([a, b]))

    # ── 거절의 두 얼굴 ───────────────────────────────────────────────

    def test_안_받았다고_해_놓고_발급이_있으면_거짓_거절이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-306"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"FALSE_REJECT": 1})
        self.assertEqual(code, 1)

    def test_이미_있다는_거절은_발급이_있는_것이_정상이다(self):
        # **이 거절은 대상에 대한 주장이다.** 그 발급은 더 앞선 **다른 접수 키**가
        # 만든 것이라 내 키로 찾으면 안 나오는 것이 정상이다. 키로만 조인하면
        # 이 정상을 결함으로 읽는다 — 축이 둘이어야 하는 이유가 여기 있다.
        other = foreign_key(ISSUANCE)
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-305"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, other)])
        self.assertEqual(self.types(report),
                         {"ALREADY_ISSUED_CONFIRMED": 1, "ORPHAN": 1})
        # 그 발급 자체는 우리가 보낸 적 없는 키를 달고 있으니 고아로 한 번 보고된다.
        self.assertEqual(
            [f["key"] for f in report["findings"] if f["type"] == "ORPHAN"], [other])
        self.assertEqual(code, 1)

    def test_이미_있다는_거절인데_내_키의_발급이면_거짓_거절이다(self):
        # 서버가 내 요청으로 발급을 만들어 놓고 409 로 답한 꼴이다. 대상만 보면
        # "이미 있으니 정상" 으로 보이는데, 키를 보면 그 발급이 **내 것**이다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-305"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"FALSE_REJECT": 1})
        self.assertEqual(code, 1)

    def test_이미_있다고_해_놓고_발급이_없으면_결함이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-305"])
        self.assertEqual(self.types(report), {"ALREADY_ISSUED_PHANTOM": 1})
        self.assertEqual(code, 1)

    def test_매진_거절_뒤_이미_있다면_거짓_거절이_아니다(self):
        # 재시도가 매진으로 한 번 튕기고 다음에 "이미 있다" 를 받았다. 뒤쪽만이
        # 발급이 있다는 증언이라, 앞쪽을 집으면 멀쩡한 발급이 거짓 거절로 잡힌다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-306",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-305"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
            histories=[(ISSUANCE, foreign_key(ISSUANCE))])
        self.assertEqual(self.types(report),
                         {"ALREADY_ISSUED_CONFIRMED": 1, "ORPHAN": 1})
        self.assertEqual(
            [f["type"] for f in report["findings"] if f["key"] == KEY],
            ["ALREADY_ISSUED_CONFIRMED"])

    def test_성공인데_예약번호를_못_읽으면_불일치다(self):
        # 201 인데 본문에 issuanceId 가 없거나 파싱이 깨졌다. 대상이 맞으니 발급은
        # 있지만 **무엇을 받았는지 확인이 안 된다** — 정상으로 세면 그것이 사라진다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MISMATCH": 1})
        self.assertIn("예약번호를 못 읽었다", report["findings"][0]["detail"])
        self.assertEqual(code, 1)

    def test_예약번호를_못_읽었고_발급도_없으면_유실이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t"])
        self.assertEqual(self.types(report), {"LOST": 1})
        self.assertIn("예약번호 없음", report["findings"][0]["detail"])
        self.assertEqual(code, 1)

    # ── 재기동 ───────────────────────────────────────────────────────

    def test_멱등이_진행중으로_멈춰_있으면_미완료다(self):
        # 처리 서버가 접수 키를 잡고 죽었다. ck_idempotency_status_targets 가
        # IN_PROGRESS 행의 회원·발급을 NULL 로 못박으므로 그 두 칸은 비어 있다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tUNKNOWN\t{KEY}\t1050"],
            idem=[(KEY, "IN_PROGRESS", "", "")])
        self.assertEqual(self.types(report), {"INCOMPLETE": 1})
        self.assertEqual(code, 4)

    def test_멱등이_완료라는데_발급이_없으면_결함이다(self):
        code, report, _ = self.check(
            [], idem=[(KEY, "DONE", MEMBER, ISSUANCE)])
        self.assertEqual(self.types(report), {"DANGLING_IDEM": 1})
        self.assertEqual(code, 1)

    # ── 늦은 등록 · 취소 경합 ────────────────────────────────────────

    def test_받은_뒤_취소된_발급은_유실이_아니다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "CANCELLED")])
        self.assertEqual(self.types(report), {"MATCHED_STATUS_CHANGED": 1})
        self.assertEqual(report["findings"][0]["detail"], f"발급 {ISSUANCE} 이 CANCELLED 다")
        self.assertEqual(code, 0)

    # ── 일부 조회 실패 ───────────────────────────────────────────────

    def test_멱등_조회가_실패해도_발급_쪽_판정은_살아_있다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            write_idem=False)
        self.assertEqual(self.types(report), {"LOST": 1})
        self.assertEqual(report["completeness"]["idempotency"], "PARTIAL")
        self.assertEqual(sorted(report["unjudged"]),
                         ["DANGLING_IDEM", "INCOMPLETE", "UNRESOLVED"])
        # 못 잰 칸이 있어도 **이미 잰 결함이 이긴다.**
        self.assertEqual(code, 1)

    def test_발급_조회가_실패하면_유실로_세지_않는다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            write_issuances=False)
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 3)

    def test_잘린_덤프는_빈_덤프가_아니다(self):
        # 센티널이 없으면 "0행" 이 아니라 "못 읽었다" 다. 이걸 안 가르면 덤프가
        # 중간에 끊긴 회차가 **전량 유실**로 보인다.
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp,
                        [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
                         f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
                        write_issuances=False)
            (f.dir / "db-issuances.tsv").write_text(
                f"{ISSUANCE}\t{ROUND}\t{MEMBER}\tISSUED\n")   # #EOF 가 없다
            code, report, _ = f.run()
        self.assertEqual(self.types(report), {})
        self.assertEqual(report["completeness"]["issuances"], "PARTIAL")
        self.assertEqual(code, 3)

    def test_행수가_어긋난_덤프도_판정하지_않는다(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp, [], write_issuances=False)
            (f.dir / "db-issuances.tsv").write_text(
                f"{ISSUANCE}\t{ROUND}\t{MEMBER}\tISSUED\n#EOF\t9\n")
            code, report, _ = f.run()
        self.assertEqual(report["completeness"]["issuances"], "PARTIAL")
        self.assertEqual(code, 3)

    # ── 기록 파일의 오염 ─────────────────────────────────────────────

    def test_남의_줄이_섞여도_판정은_안_바뀐다(self):
        code, report, _ = self.check(
            ["k6 가 낸 아무 줄", f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tOK\t{KEY}\t{ISSUANCE}", "또 한 줄"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(report["totals"]["foreign_lines"], 2)
        self.assertEqual(code, 0)

    def test_칸_수가_틀린_기록은_형식_깨짐으로_세고_판정을_멈춘다(self):
        code, report, _ = self.check([f"CY960\tREQ\t{KEY}\t{ROUND}"])
        self.assertEqual(report["totals"]["malformed_lines"], 1)
        self.assertEqual(report["completeness"]["records"], "PARTIAL")
        self.assertEqual(code, 3)

    def test_기록_파일이_없으면_아무것도_판정하지_않는다(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp, [], [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
            (f.dir / "requests.log").unlink()
            code, report, _ = f.run()
        self.assertEqual(report["completeness"]["records"], "MISSING")
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 3)

    def test_기록을_끈_회차를_대조하면_판정하지_않는다(self):
        # --console-output 은 기록을 꺼도 파일을 만든다. 그 빈 파일을 "요청 0건" 으로
        # 읽으면 회차의 발급이 **전부 고아**로 보고된다 — 멀쩡한 회차에서 만 건짜리
        # 거짓 결함이 난다. 표식이 없으면 판정하지 않는 이유가 이것이다.
        code, report, _ = self.check(
            [], [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")], marker=False)
        self.assertEqual(self.types(report), {})
        self.assertEqual(report["completeness"]["records"], "MISSING")
        self.assertEqual(code, 3)

    def test_다른_회차의_표식만_있으면_판정하지_않는다(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp, [], [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")], marker=False)
            (f.dir / "requests.log").write_text(f"CY960\tRUN\t{ROUND + 1}\n")
            code, report, _ = f.run()
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 3)

    def test_앞_회차의_기록이_남아_있으면_걸러_낸다(self):
        # --console-output 은 덮어쓰지 않고 이어 쓴다(실측). 앞 회차의 키는 이번 회차
        # DB 에 없으니, 안 거르면 전부 미해결로 보인다.
        old_key = "99999999-8888-4777-8666-555555555555"
        code, report, _ = self.check(
            [f"CY960\tREQ\t{old_key}\t{ROUND - 1}\t{MEMBER}\t{GRADE}",
             f"CY960\tOK\t{old_key}\t{ISSUANCE - 1}",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
             f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(report["totals"]["other_round_records"], 1)
        self.assertEqual(code, 0)

    # ── 묶음 (CY-964) ────────────────────────────────────────────────

    def _rep(self, root, name, records, issuances=(), idem=(), histories=None):
        d = Path(root) / name
        d.mkdir(parents=True)
        Fixture(d, records, issuances, idem, histories=histories)
        return d

    def batch(self, *dirs):
        proc = subprocess.run([sys.executable, str(CLI), *[str(d) for d in dirs]],
                              capture_output=True, text=True)
        return proc.returncode, proc

    def test_반복을_여럿_주면_전부_돈다(self):
        ok = [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"]
        with tempfile.TemporaryDirectory() as tmp:
            a = self._rep(tmp, "rep-1", ok, [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
            b = self._rep(tmp, "rep-2", ok, [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
            code, proc = self.batch(a, b)
            written = [len(list(d.glob("reconcile-*.json"))) for d in (a, b)]
        self.assertEqual(written, [1, 1])          # 반복마다 자기 보고서가 남는다
        self.assertIn("묶음 2개 — 정상 2", proc.stdout)
        self.assertEqual(code, 0)

    def test_하나가_결함이어도_나머지를_판정한다(self):
        # 첫 반복에서 멈추면 나머지를 못 본다. 결함 반복을 **앞에** 둔다.
        with tempfile.TemporaryDirectory() as tmp:
            bad = self._rep(tmp, "rep-1",
                            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
                             f"CY960\tOK\t{KEY}\t{ISSUANCE}"])           # 발급 없음 → LOST
            good = self._rep(tmp, "rep-2",
                             [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
                              f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
                             [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
            code, proc = self.batch(bad, good)
            after = len(list(good.glob("reconcile-*.json")))
        self.assertEqual(after, 1, "뒤 반복이 판정되지 않았다")
        self.assertIn("묶음 2개", proc.stdout)
        self.assertEqual(code, 1)

    def test_묶음의_종료코드는_가장_나쁜_것이다(self):
        # **크기순이 아니다.** 결함(1)이 판정 불가(3)·보류(4)보다 나쁘다.
        self.assertEqual(reconcile_mod.worst_code([0, 4, 3, 1]), 1)
        self.assertEqual(reconcile_mod.worst_code([0, 4, 3]), 3)
        self.assertEqual(reconcile_mod.worst_code([0, 4]), 4)
        self.assertEqual(reconcile_mod.worst_code([0, 0]), 0)
        self.assertEqual(reconcile_mod.worst_code([]), 0)

    def test_보류와_결함이_섞이면_결함이_이긴다(self):
        with tempfile.TemporaryDirectory() as tmp:
            pending = self._rep(tmp, "rep-1",
                                [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}"])  # UNRESOLVED
            defect = self._rep(tmp, "rep-2",
                               [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
                                f"CY960\tOK\t{KEY}\t{ISSUANCE}"])            # LOST
            code, proc = self.batch(pending, defect)
        self.assertIn("결함 1", proc.stdout)
        self.assertIn("보류 1", proc.stdout)
        self.assertEqual(code, 1)

    def test_묶음에는_out_을_못_쓴다(self):
        # 반복마다 자기 보고서가 남아야 한다. 한 경로로 몰면 서로 덮어쓴다.
        with tempfile.TemporaryDirectory() as tmp:
            a = self._rep(tmp, "rep-1", [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}"])
            b = self._rep(tmp, "rep-2", [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}"])
            proc = subprocess.run(
                [sys.executable, str(CLI), str(a), str(b), "--out", f"{tmp}/x.json"],
                capture_output=True, text=True)
        self.assertEqual(proc.returncode, 2)          # argparse 사용법 오류
        self.assertIn("--out 은 반복 하나일 때만", proc.stderr)

    # ── 최신 보고서 고르기 (CY-965) ──────────────────────────────────

    def test_같은_초의_보고서는_접미사_번호로_가른다(self):
        # **사전순으로 고르면 틀린다** — '-'(0x2D) < '.'(0x2E) 라 `-2` 가 접미사 없는
        # 것보다 앞서고, `-10` 은 `-2` 보다도 앞선다(문자열 비교).
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            same = "2026-09-13T03:45:12+00:00"
            for name, mark in (("reconcile-20260913T034512+0000.json", "첫째"),
                               ("reconcile-20260913T034512+0000-2.json", "둘째"),
                               ("reconcile-20260913T034512+0000-10.json", "열째")):
                (d / name).write_text(json.dumps(as_report(same, mark)))
            self.assertEqual(reconcile_mod.latest_report(d)["mark"], "열째")
            self.assertEqual(
                [p.name for p in reconcile_mod.report_paths(d)],
                ["reconcile-20260913T034512+0000.json",
                 "reconcile-20260913T034512+0000-2.json",
                 "reconcile-20260913T034512+0000-10.json"])

    def test_시각이_다르면_시각으로_고른다(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "reconcile-20260913T034512+0000-9.json").write_text(
                json.dumps(as_report("2026-09-13T03:45:12+00:00", "이른")))
            (d / "reconcile-20260913T034513+0000.json").write_text(
                json.dumps(as_report("2026-09-13T03:45:13+00:00", "늦은")))
            self.assertEqual(reconcile_mod.latest_report(d)["mark"], "늦은")

    def test_보고서가_없으면_None_이다(self):
        # **"대조 안 함" 과 "대조했고 깨끗함" 은 다르다.** 빈 결과로 뭉개면 안 한 것이
        # 깨끗한 것으로 보인다.
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(reconcile_mod.latest_report(Path(tmp)))

    def test_옛_보고서가_깨져도_최신을_고른다(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "reconcile-20260913T034512+0000.json").write_text("{ 깨짐")
            (d / "reconcile-20260913T034513+0000.json").write_text(
                json.dumps(as_report("2026-09-13T03:45:13+00:00", "멀쩡")))
            self.assertEqual(reconcile_mod.latest_report(d)["mark"], "멀쩡")

    def test_최신_보고서가_깨지면_옛것으로_물러서지_않는다(self):
        # 최신 쓰기가 잘린 상황이다. **옛 결과를 최신으로 내면 그 사이에 생긴 유실이
        # 사라지고, 요약이 옛 보고서로 OK 를 찍는다.** 못 읽었다고 말해야 한다.
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "reconcile-20260913T034512+0000.json").write_text(
                json.dumps(as_report("2026-09-13T03:45:12+00:00", "이른")))
            (d / "reconcile-20260913T034513+0000.json").write_text("{ 잘림")
            got = reconcile_mod.latest_report(d)
        self.assertIn("unreadable", got)
        self.assertIn("034513", got["unreadable"])

    def test_보고서가_아닌_JSON_을_깨끗한_대조로_안_본다(self):
        # `{}` 도 유효한 JSON 이다. 그대로 접으면 유형이 하나도 없으니 **깨끗한
        # 대조로 보인다** — 이름만 맞는 남의 파일도 마찬가지다.
        cases = [
            ("빈 객체", {}),
            ("스키마 없음", {"counts": {}, "unjudged": []}),
            ("스키마 다름", {"schema": "남의것/1", "counts": {}, "unjudged": []}),
            ("counts 없음", {"schema": reconcile_mod.SCHEMA, "unjudged": []}),
            ("counts 가 목록", {"schema": reconcile_mod.SCHEMA, "counts": [], "unjudged": []}),
            ("unjudged 가 사전", {"schema": reconcile_mod.SCHEMA, "counts": {}, "unjudged": {}}),
            ("객체가 아님", [1, 2, 3]),
            # 그릇만 보면 모자란다. 셋 다 다른 방식으로 나쁘다 — 재서 확인했다.
            ("counts 값이 문자열",                       # 접다가 TypeError 로 요약이 죽는다
             {"schema": reconcile_mod.SCHEMA, "counts": {"LOST": "1"}, "unjudged": []}),
            ("counts 값이 음수",
             {"schema": reconcile_mod.SCHEMA, "counts": {"LOST": -1}, "unjudged": []}),
            ("counts 값이 bool",                          # True 를 1건으로 세면 안 된다
             {"schema": reconcile_mod.SCHEMA, "counts": {"LOST": True}, "unjudged": []}),
            ("counts 키가 숫자",                          # 안 터지고 **조용히 사라진다**
             {"schema": reconcile_mod.SCHEMA, "counts": {7: 1}, "unjudged": []}),
            ("counts 키가 모르는 이름",
             {"schema": reconcile_mod.SCHEMA, "counts": {"LOSTT": 1}, "unjudged": []}),
            ("unjudged 에 숫자",                          # 정렬에서 죽는다
             {"schema": reconcile_mod.SCHEMA, "counts": {}, "unjudged": ["LOST", 3]}),
            ("unjudged 에 모르는 이름",
             {"schema": reconcile_mod.SCHEMA, "counts": {}, "unjudged": ["LOSTT"]}),
        ]
        for label, payload in cases:
            with self.subTest(label):
                with tempfile.TemporaryDirectory() as tmp:
                    d = Path(tmp)
                    (d / "reconcile-20260913T034512+0000.json").write_text(json.dumps(payload))
                    got = reconcile_mod.latest_report(d)
                self.assertIn("unreadable", got, label)

    def test_아는_유형과_정수면_통과한다(self):
        ok = {"schema": reconcile_mod.SCHEMA,
              "counts": {"LOST": 3, "MATCHED": 0}, "unjudged": ["ORPHAN"]}
        self.assertIsNone(reconcile_mod.report_shape_problem(ok))

    def test_모양이_맞으면_그대로_낸다(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "reconcile-20260913T034512+0000.json").write_text(
                json.dumps(as_report("2026-09-13T03:45:12+00:00", "정상")))
            got = reconcile_mod.latest_report(d)
        self.assertNotIn("unreadable", got)
        self.assertEqual(got["mark"], "정상")

    def test_진짜_대조가_낸_보고서는_모양_검사를_통과한다(self):
        # 손으로 만든 픽스처만 통과하면 검사가 실물과 어긋난 것이다.
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp, [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}"])
            subprocess.run([sys.executable, str(CLI), str(f.dir)],
                           capture_output=True, text=True)
            got = reconcile_mod.latest_report(f.dir)
        self.assertIsNotNone(got)
        self.assertNotIn("unreadable", got)
        self.assertIsNone(reconcile_mod.report_shape_problem(got))

    def test_순서는_파일_내용을_안_읽고_정한다(self):
        # 내용으로 정렬하면 못 읽은 파일의 자리를 정할 수 없다. 이름만으로 정한다.
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            for n in ("reconcile-20260913T034512+0000.json",
                      "reconcile-20260913T034512+0000-2.json",
                      "reconcile-20260913T034513+0000.json"):
                (d / n).write_text("{ 전부 깨짐")
            self.assertEqual(
                [p.name for p in reconcile_mod.report_paths(d)],
                ["reconcile-20260913T034512+0000.json",
                 "reconcile-20260913T034512+0000-2.json",
                 "reconcile-20260913T034513+0000.json"])

    def test_대조가_실제로_남긴_파일도_골라낸다(self):
        # 위 시험들은 이름을 손으로 만들었다. **진짜 대조가 낸 파일**로도 도는지 본다 —
        # 이름 규칙이 바뀌면 손으로 만든 픽스처만 통과하는 상태가 된다.
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp, [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}"])
            for _ in range(2):
                subprocess.run([sys.executable, str(CLI), str(f.dir)],
                               capture_output=True, text=True)
            paths = reconcile_mod.report_paths(f.dir)
            latest = reconcile_mod.latest_report(f.dir)
            # 고른 것이 정말 마지막 파일의 내용인지 본다.
            tail = json.loads(paths[-1].read_text())
            names = [x.name for x in paths]
        self.assertEqual(len(paths), 2, names)
        self.assertIsNotNone(latest)
        self.assertEqual(latest, tail)
        # ⚠️ **접미사가 붙는다고 단언하지 않는다.** 두 실행이 초 경계를 넘으면
        #    접미사 없이 시각만 달라져, 그 단언은 간헐적으로 빨개진다. 접미사 순서
        #    자체는 위의 손 픽스처 시험이 결정적으로 태운다. 여기서 볼 것은
        #    **진짜 이름도 파싱된다**는 것뿐이다.
        self.assertTrue(all(n.startswith("reconcile-") for n in names), names)

    # ── 전후 비교 ────────────────────────────────────────────────────

    def _report(self, tmp, name, records, issuances=(), idem=(), round_id=ROUND):
        d = Path(tmp) / name
        d.mkdir()
        f = Fixture(d, records, issuances, idem, round_id=round_id)
        f.run()
        return d / "report.json"

    def test_다시_대조해서_해소되면_해소로_나온다(self):
        records = [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
                   f"CY960\tUNKNOWN\t{KEY}\t1050"]
        with tempfile.TemporaryDirectory() as tmp:
            before = self._report(tmp, "before", records)
            after = self._report(tmp, "after", records,
                                 [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
            proc = subprocess.run(
                [sys.executable, str(CLI), "--diff", str(before), str(after)],
                capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("UNRESOLVED", proc.stdout)
        self.assertIn("해소 1", proc.stdout)

    def test_고아도_전후_비교에서_서로_구분된다(self):
        # 고아에는 접수 키가 없다. 그렇다고 빈 문자열로 두면 열 건이든 한 건이든
        # 집합에서 한 원소로 뭉개져, 해소된 고아와 새로 생긴 고아가 안 보인다.
        with tempfile.TemporaryDirectory() as tmp:
            before = self._report(tmp, "before", [],
                                  [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED"),
                                   issuance(ISSUANCE + 1, ROUND, MEMBER + 1, "ISSUED")])
            after = self._report(tmp, "after", [],
                                 [issuance(ISSUANCE + 1, ROUND, MEMBER + 1, "ISSUED"),
                                  issuance(ISSUANCE + 2, ROUND, MEMBER + 2, "ISSUED")])
            proc = subprocess.run(
                [sys.executable, str(CLI), "--diff", str(before), str(after)],
                capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("잔여 1 · 신규 1 · 해소 1", proc.stdout)

    def test_한쪽이_판정_못_한_유형은_비교하지_않는다(self):
        # before 는 이력을 못 읽어 고아를 판정하지 못했고 after 는 판정했다. 그 목록을
        # 집합으로 빼면 고아가 전부 "신규" 로 나온다 — 아무 일도 안 일어났는데 새로
        # 생긴 것처럼 보인다.
        with tempfile.TemporaryDirectory() as tmp:
            b = Path(tmp) / "before"; b.mkdir()
            Fixture(b, [], [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")],
                    write_histories=False).run()
            a = Path(tmp) / "after"; a.mkdir()
            Fixture(a, [], [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")]).run()
            proc = subprocess.run(
                [sys.executable, str(CLI), "--diff",
                 str(b / "report.json"), str(a / "report.json")],
                capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("ORPHAN", proc.stdout)
        self.assertIn("비교 불가", proc.stdout)
        self.assertNotIn("신규 1", proc.stdout)

    def test_해소를_복구_원인으로_말하지_않는다(self):
        """요구사항 §6.3 「복구 증거 구분」.

        > 전후 차이는 **상태 변화의 증거다.** 자동 복구를 주장하려면 해당 키의
        > 재시도·처리 이력도 확인.

        미해결이 지워진 이유는 늦은 등록일 수도, 다른 경로의 보상일 수도, 사람이
        손댄 것일 수도 있다 — **스냅샷 둘로는 못 가른다.**
        """
        records = [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
                   f"CY960\tUNKNOWN\t{KEY}\t1050"]
        with tempfile.TemporaryDirectory() as tmp:
            before = self._report(tmp, "before", records)
            after = self._report(tmp, "after", records,
                                 [issuance(ISSUANCE, ROUND, MEMBER, "ISSUED")])
            proc = subprocess.run(
                [sys.executable, str(CLI), "--diff", str(before), str(after)],
                capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("해소 1", proc.stdout)
        # 변화는 말하되 **원인은 말하지 않는다.**
        self.assertIn("왜 변했는지는 이 표가 말하지 않는다", proc.stdout)
        self.assertIn("issuance_histories", proc.stdout)
        self.assertNotIn("늦게 들어온 등록이고", proc.stdout)

    def test_해소가_없으면_그_안내를_안_낸다(self):
        # 할 말이 없을 때 경고를 붙이면 다음부터 아무도 안 읽는다.
        records = [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}",
                   f"CY960\tUNKNOWN\t{KEY}\t1050"]
        with tempfile.TemporaryDirectory() as tmp:
            a = self._report(tmp, "a", records)
            b = self._report(tmp, "b", records)
            proc = subprocess.run(
                [sys.executable, str(CLI), "--diff", str(a), str(b)],
                capture_output=True, text=True)
        self.assertNotIn("왜 변했는지는", proc.stdout)

    def test_회차가_다르면_키_단위로_비교하지_않는다(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = self._report(tmp, "a", [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}"])
            b = self._report(tmp, "b", [f"CY960\tREQ\t{KEY}\t{ROUND + 1}\t{MEMBER}\t{GRADE}"],
                             round_id=ROUND + 1)
            proc = subprocess.run(
                [sys.executable, str(CLI), "--diff", str(a), str(b)],
                capture_output=True, text=True)
        self.assertIn("회차가 다르다", proc.stdout)
        self.assertNotIn("잔여", proc.stdout)

    # ── 두 번째 멱등 구간 (CY-974) ───────────────────────────────────
    #
    # 여기 시험은 **독립 기록을 안 흔든다.** 이 구간은 DB 안의 두 테이블끼리
    # 맞대는 것이라, 기록을 건드리면 무엇이 판정을 눌렀는지 흐려진다.

    def sent(self, key=KEY, member=MEMBER, iid=1):
        """성공 응답 한 건과 그 발급. 이 구간의 시험은 전부 여기서 출발한다."""
        return ([f"CY960\tREQ\t{key}\t{ROUND}\t{member}\t{GRADE}",
                 f"CY960\tOK\t{key}\t{iid}"],
                [issuance(iid, ROUND, member, "ISSUED")])

    def test_발급마다_알림과_아웃박스가_있으면_정상이다(self):
        records, issuances = self.sent()
        code, report, _ = self.check(records, issuances)
        self.assertEqual(code, 0)
        self.assertEqual(self.types(report), {"MATCHED": 1})
        # **판정 불가로 빠져서 초록인 것이 아니어야 한다.** 넷이 실제로 판정됐다.
        self.assertEqual(report["unjudged"], [])

    def test_발급이_있는데_알림이_없으면_결함이다(self):
        """#326 의 형상. V2 회차가 알림을 한 건도 안 만들고 있었다."""
        records, issuances = self.sent()
        code, report, _ = self.check(records, issuances, notifications=[])
        self.assertEqual(code, 1)
        self.assertEqual(self.types(report),
                         {"MATCHED": 1, "NOTIFICATION_MISSING": 1})

    def test_늦게_취소된_발급도_알림이_있어야_한다(self):
        """조건은 **발급 행의 존재**이지 상태가 아니다.

        알림은 발급 시점에 만들어진다. 상태로 거르면 그 뒤에 취소·사용된 건이
        조용히 면제되고, 그 회차의 알림 누락이 안 보인다.
        """
        records, _ = self.sent()
        code, report, _ = self.check(
            records, [issuance(1, ROUND, MEMBER, "CANCELLED")], notifications=[])
        self.assertEqual(code, 1)
        self.assertEqual(report["counts"]["NOTIFICATION_MISSING"], 1)

    def test_다른_회차의_발급은_알림을_안_따진다(self):
        """예약번호로 끌어온 회차 밖 발급까지 여기서 세면 안 된다.

        그 건의 결함은 첫 구간이 TARGET_MISMATCH 로 이미 보고한다 — 같은 행을
        두 이름으로 두 번 세면 다음 사람이 결함을 두 배로 읽는다.
        """
        records, issuances = self.sent()
        code, report, _ = self.check(
            records, issuances + [issuance(9, ROUND + 1, MEMBER + 1, "ISSUED")],
            notifications=derive_notifications(issuances))
        self.assertEqual(report["counts"]["NOTIFICATION_MISSING"], 0)

    def test_알림은_있는데_아웃박스가_없으면_결함이다(self):
        """트랜잭션이 반만 들어간 꼴. 외부로 나갈 길이 아예 없다."""
        records, issuances = self.sent()
        code, report, _ = self.check(records, issuances, notifications=[
            (1 + NOTIFICATION_ID_BASE, 1, ROUND, MEMBER, "PENDING", "", "", "")])
        self.assertEqual(code, 1)
        self.assertEqual(report["counts"]["OUTBOX_MISSING"], 1)
        self.assertIn("아웃박스가 아예 없다",
                      [f["detail"] for f in report["findings"]])

    def test_첫_시도_아웃박스가_없으면_결함이다(self):
        """외부 키의 뒷자리가 attempt_seq 다. `(1, INITIAL)` 이 그 시작이고,
        그것 없이 2번째 시도만 있으면 시작을 건너뛴 것이다."""
        records, issuances = self.sent()
        code, report, _ = self.check(records, issuances, notifications=[
            (1 + NOTIFICATION_ID_BASE, 1, ROUND, MEMBER, "PENDING",
             2, "MANUAL", "PENDING")])
        self.assertEqual(code, 1)
        self.assertEqual(report["counts"]["OUTBOX_MISSING"], 1)

    def test_수동_재처리로_아웃박스가_둘이어도_정상이다(self):
        """**여분은 결함이 아니다.** 승인된 재처리는 새 시도를 정당하게 만든다 —
        없는 것만 결함이라고 못 박아 두지 않으면 정상 운영이 빨개진다."""
        records, issuances = self.sent()
        code, report, _ = self.check(records, issuances, notifications=[
            (1 + NOTIFICATION_ID_BASE, 1, ROUND, MEMBER, "SENT",
             1, "INITIAL", "PUBLISHED"),
            (1 + NOTIFICATION_ID_BASE, 1, ROUND, MEMBER, "SENT",
             2, "MANUAL", "PENDING")])
        self.assertEqual(code, 0)
        self.assertEqual(self.types(report), {"MATCHED": 1})

    def test_알림의_대상이_발급과_다르면_결함이다(self):
        """알림이 엉뚱한 사람에게 간다. 발급은 멀쩡해서 첫 구간은 아무 말도 안 한다."""
        records, issuances = self.sent()
        code, report, _ = self.check(records, issuances, notifications=[
            (1 + NOTIFICATION_ID_BASE, 1, ROUND, MEMBER + 7, "PENDING",
             1, "INITIAL", "PENDING")])
        self.assertEqual(code, 1)
        self.assertEqual(self.types(report),
                         {"MATCHED": 1, "NOTIFICATION_TARGET_MISMATCH": 1})

    def test_알림이_가리키는_발급이_없으면_고아다(self):
        records, issuances = self.sent()
        code, report, _ = self.check(records, issuances, notifications=[
            (1 + NOTIFICATION_ID_BASE, 1, ROUND, MEMBER, "PENDING",
             1, "INITIAL", "PENDING"),
            (2 + NOTIFICATION_ID_BASE, 777, ROUND, MEMBER + 1, "PENDING",
             1, "INITIAL", "PENDING")])
        self.assertEqual(code, 1)
        self.assertEqual(report["counts"]["NOTIFICATION_ORPHAN"], 1)

    def test_고아이면서_아웃박스도_없으면_둘_다_센다(self):
        """**서로 다른 사실 둘이다.** elif 로 이으면 앞 갈래가 뒤를 삼킨다."""
        records, issuances = self.sent()
        code, report, _ = self.check(records, issuances, notifications=[
            (1 + NOTIFICATION_ID_BASE, 1, ROUND, MEMBER, "PENDING",
             1, "INITIAL", "PENDING"),
            (2 + NOTIFICATION_ID_BASE, 777, ROUND, MEMBER + 1, "PENDING",
             "", "", "")])
        self.assertEqual(report["counts"]["NOTIFICATION_ORPHAN"], 1)
        self.assertEqual(report["counts"]["OUTBOX_MISSING"], 1)

    def test_같은_유형의_알림_결함_둘이_한_원소로_뭉개지지_않는다(self):
        """전후 비교는 **판정의 key 로** 잔여·신규·해소를 가른다.

        이름을 대상(회차/회원)으로 붙이면 같은 대상의 둘이 한 원소가 되어, 하나가
        고쳐졌는데도 "잔여" 로 남는다 — 고아 판정이 이미 밟은 함정이다.
        """
        records = [f"CY960\tREQ\tk{i}\t{ROUND}\t{MEMBER}\t{GRADE}" for i in (1, 2)]
        records += [f"CY960\tOK\tk{i}\t{i}" for i in (1, 2)]
        code, report, _ = self.check(
            records,
            [issuance(1, ROUND, MEMBER, "ISSUED"), issuance(2, ROUND, MEMBER, "ISSUED")],
            notifications=[])
        keys = [f["key"] for f in report["findings"]
                if f["type"] == "NOTIFICATION_MISSING"]
        self.assertEqual(len(set(keys)), 2, keys)

    def test_다른_회차의_발급을_가리키는_알림은_고아가_아니라_대상_불일치다(self):
        """**이름이 틀리면 고치는 사람이 엉뚱한 데를 판다.**

        그 발급이 덤프에 있으면 "가리키는 발급이 없다"(고아)가 아니라 "알림의
        대상이 그 발급과 다르다"(대상 불일치)가 맞는 이름이다. 그래서 reconcile.sh
        가 알림이 가리키는 발급을 발급 덤프로 함께 끌어온다 — 이 시험은 그 덤프가
        왔을 때 이름이 제대로 붙는지를 본다.
        """
        records, issuances = self.sent()
        code, report, _ = self.check(
            records,
            issuances + [issuance(9, ROUND + 1, MEMBER + 1, "ISSUED")],
            notifications=derive_notifications(issuances) + [
                (9 + NOTIFICATION_ID_BASE, 9, ROUND, MEMBER + 1, "PENDING",
                 1, "INITIAL", "PENDING")])
        self.assertEqual(code, 1)
        self.assertEqual(report["counts"]["NOTIFICATION_TARGET_MISMATCH"], 1)
        self.assertEqual(report["counts"]["NOTIFICATION_ORPHAN"], 0)

    def test_알림_덤프가_없으면_그_넷만_판정_불가다(self):
        """첫 구간의 판정은 살아 있어야 한다. 한 덤프가 빠졌다고 전부
        판정 불가로 내리면 **가드가 진짜 검출을 덮는다.**"""
        records, issuances = self.sent()
        code, report, _ = self.check(records, issuances,
                                     write_notifications=False)
        self.assertEqual(code, 3)
        self.assertEqual(set(report["unjudged"]),
                         set(reconcile_mod.NOTIFICATION_TYPES))
        self.assertEqual(report["counts"]["MATCHED"], 1)

    def test_기록이_깨져도_알림_판정은_산다(self):
        """이 구간은 독립 기록을 안 쓴다. k6 가 죽은 반복에서도 알림 결함은
        여전히 보여야 한다 — 여기를 기록에 매어 두면 조용히 사라진다."""
        records, issuances = self.sent()
        code, report, _ = self.check(records, issuances, notifications=[],
                                     measure_attempts=99)
        # 기록이 어긋나 첫 구간은 판정을 멈췄는데
        self.assertEqual(report["completeness"]["records"], "PARTIAL")
        self.assertNotIn("MATCHED", report["counts"])
        # 알림 누락은 그대로 잡힌다
        self.assertEqual(report["counts"]["NOTIFICATION_MISSING"], 1)
        self.assertEqual(code, 1)

    def test_발급_덤프가_깨져도_아웃박스_누락은_잡는다(self):
        """OUTBOX_MISSING 은 알림 ↔ 아웃박스만 본다. 발급 덤프에 매어 두면
        발급 조회가 실패한 회차에서 이 결함이 통째로 사라진다."""
        records, issuances = self.sent()
        code, report, _ = self.check(
            records, issuances, write_issuances=False, notifications=[
                (1 + NOTIFICATION_ID_BASE, 1, ROUND, MEMBER, "PENDING",
                 "", "", "")])
        self.assertEqual(report["counts"]["OUTBOX_MISSING"], 1)
        self.assertIn("NOTIFICATION_MISSING", report["unjudged"])

    # ── 덮어쓰지 않는다 ──────────────────────────────────────────────

    def test_대조_결과는_실행마다_다른_파일로_남는다(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp, [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}\t{GRADE}"])
            for _ in range(2):
                subprocess.run([sys.executable, str(CLI), str(f.dir)],
                               capture_output=True, text=True)
            written = sorted(f.dir.glob("reconcile-*.json"))
        # **같은 초에 두 번 돌아도 둘 다 남아야 한다.** 덮어쓰지 않겠다는 것이
        # 이 기능의 약속이고, 초 단위 이름은 그 약속을 그 자리에서 깬다.
        self.assertEqual(len(written), 2, [p.name for p in written])
        self.assertTrue(all(p.name.startswith("reconcile-") for p in written))


if __name__ == "__main__":
    unittest.main(verbosity=2)
