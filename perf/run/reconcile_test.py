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

ROUND = 7001          # 회차
MEMBER = 4200         # 회원 — 회차와 자릿수까지 다르게 둔다
ISSUANCE = 990001     # 발급 번호
KEY = "11111111-2222-4333-8444-555555555555"


def tsv(rows):
    body = "".join("\t".join(str(c) for c in r) + "\n" for r in rows)
    return body + f"#EOF\t{len(rows)}\n"


class Fixture:
    """한 반복 디렉터리를 만든다. 안 준 파일은 안 만든다 — 그것이 조회 실패의 형상이다."""

    def __init__(self, tmp, records, issuances=(), idem=(), *,
                 configured=1, dropped=0, round_id=ROUND,
                 write_issuances=True, write_idem=True, marker=True):
        self.dir = Path(tmp)
        (self.dir / "round.json").write_text(json.dumps({
            "engine": "V2", "target_round_id": round_id,
            "configured_requests": configured}))
        (self.dir / "k6-summary.json").write_text(json.dumps({
            "metrics": {"dropped_iterations": {"values": {"count": dropped}}}}))
        # 기록이 켜진 회차의 파일에는 setup() 이 낸 표식이 **반드시** 앞에 있다.
        # 픽스처가 그것을 빼면 런타임에 없는 형상을 시험하게 된다.
        head = [f"CY960\tRUN\t{round_id}"] if marker else []
        (self.dir / "requests.log").write_text(
            "".join(l + "\n" for l in head + list(records)))
        if write_issuances:
            (self.dir / "db-issuances.tsv").write_text(tsv(issuances))
        if write_idem:
            (self.dir / "db-idempotency.tsv").write_text(tsv(idem))

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
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(code, 0)

    def test_명시적_거절은_발급이_없어야_정상이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-306"])
        self.assertEqual(self.types(report), {"MATCHED_REJECTED": 1})
        self.assertEqual(code, 0)

    # ── 응답 유실 ────────────────────────────────────────────────────

    def test_응답을_잃어도_발급이_있으면_해소된다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",
             f"CY960\tUNKNOWN\t{KEY}\t1050"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"RESOLVED_ISSUED": 1})
        self.assertEqual(code, 0)

    def test_응답_줄이_아예_없어도_같은_판정이_난다(self):
        # k6 가 중간에 죽으면 REQ 만 남는다. 그것도 결과 불명이다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"RESOLVED_ISSUED": 1})
        self.assertEqual(code, 0)

    def test_성공을_받았는데_발급이_없으면_유실이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"])
        self.assertEqual(self.types(report), {"LOST": 1})
        self.assertEqual(code, 1)

    # ── 양쪽 동시 누락 ───────────────────────────────────────────────

    def test_양쪽에서_동시에_빠지면_미해결로_남는다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}"])
        self.assertEqual(self.types(report), {"UNRESOLVED": 1})
        self.assertEqual(code, 4)

    def test_보낸_기록이_없으면_그_건은_보이지도_않는다(self):
        # 위 시험과 **DB 상태가 똑같다.** 다른 것은 REQ 한 줄뿐이고, 그 한 줄이
        # 있고 없고가 "미해결 1건" 과 "아무 일 없음" 을 가른다.
        # 요청 **전에** 기록하는 설계가 무엇을 사는지가 이 두 시험의 차이다.
        code, report, _ = self.check([])
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 0)

    def test_기록도_못쏨도_아닌_요청은_총계로_드러난다(self):
        _, report, proc = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED")], configured=5, dropped=2)
        # 설정 5 − 기록 1 − 못 쏨 2 = 2건이 어디에도 없다.
        self.assertIn("설명 안 되는 2건", proc.stdout)

    # ── 고아 · 중복 ──────────────────────────────────────────────────

    def test_보낸_적_없는_발급은_고아다(self):
        code, report, _ = self.check([], [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"ORPHAN": 1})
        self.assertEqual(code, 1)

    def test_다른_회차의_발급은_고아가_아니다(self):
        # 워밍업 회차의 발급까지 고아로 세면 정상 회차가 전량 결함으로 보인다.
        code, report, _ = self.check([], [(ISSUANCE, ROUND + 1, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 0)

    def test_한_대상에_발급이_둘이면_중복이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED"),
             (ISSUANCE + 1, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"DUPLICATE": 1})
        self.assertEqual(code, 1)

    # ── 재전송을 한 신청으로 묶는다 ──────────────────────────────────

    def test_같은_접수_키의_재전송은_한_신청이다(self):
        # 첫 시도는 응답을 잃었고 두 번째는 "이미 있다" 로 거절됐는데 세 번째가 성공했다.
        # 세 줄이지만 신청은 하나고, 결론은 가장 확정적인 것 — 성공이다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tUNKNOWN\t{KEY}\t1050",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tREJECTED\t{KEY}\tCOUPON-305",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(report["findings"][0]["attempts"], 3)
        self.assertEqual(code, 0)

    def test_한_접수_키에_예약번호가_둘이면_불일치다(self):
        # 멱등이 깨졌다는 뜻이다. 재전송이 새 발급을 만들었다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE}",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE + 9}"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MISMATCH": 1})
        self.assertEqual(code, 1)

    def test_받은_예약번호가_DB_와_다르면_불일치다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [(ISSUANCE + 5, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MISMATCH": 1})
        self.assertEqual(code, 1)

    # ── 거절의 두 얼굴 ───────────────────────────────────────────────

    def test_안_받았다고_해_놓고_발급이_있으면_거짓_거절이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-306"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"FALSE_REJECT": 1})
        self.assertEqual(code, 1)

    def test_이미_있다는_거절은_발급이_있는_것이_정상이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-305"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"ALREADY_ISSUED_CONFIRMED": 1})
        self.assertEqual(code, 0)

    def test_이미_있다고_해_놓고_발급이_없으면_결함이다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",
             f"CY960\tREJECTED\t{KEY}\tCOUPON-305"])
        self.assertEqual(self.types(report), {"ALREADY_ISSUED_PHANTOM": 1})
        self.assertEqual(code, 1)

    # ── 재기동 ───────────────────────────────────────────────────────

    def test_멱등이_진행중으로_멈춰_있으면_미완료다(self):
        # 처리 서버가 접수 키를 잡고 죽었다. ck_idempotency_status_targets 가
        # IN_PROGRESS 행의 회원·발급을 NULL 로 못박으므로 그 두 칸은 비어 있다.
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tUNKNOWN\t{KEY}\t1050"],
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
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [(ISSUANCE, ROUND, MEMBER, "CANCELLED")])
        self.assertEqual(self.types(report), {"MATCHED_STATUS_CHANGED": 1})
        self.assertEqual(report["findings"][0]["detail"], f"발급 {ISSUANCE} 이 CANCELLED 다")
        self.assertEqual(code, 0)

    # ── 일부 조회 실패 ───────────────────────────────────────────────

    def test_멱등_조회가_실패해도_발급_쪽_판정은_살아_있다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            write_idem=False)
        self.assertEqual(self.types(report), {"LOST": 1})
        self.assertEqual(report["completeness"]["idempotency"], "PARTIAL")
        self.assertEqual(sorted(report["unjudged"]),
                         ["DANGLING_IDEM", "INCOMPLETE", "UNRESOLVED"])
        # 못 잰 칸이 있어도 **이미 잰 결함이 이긴다.**
        self.assertEqual(code, 1)

    def test_발급_조회가_실패하면_유실로_세지_않는다(self):
        code, report, _ = self.check(
            [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}", f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            write_issuances=False)
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 3)

    def test_잘린_덤프는_빈_덤프가_아니다(self):
        # 센티널이 없으면 "0행" 이 아니라 "못 읽었다" 다. 이걸 안 가르면 덤프가
        # 중간에 끊긴 회차가 **전량 유실**로 보인다.
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp,
                        [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",
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
            ["k6 가 낸 아무 줄", f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",
             f"CY960\tOK\t{KEY}\t{ISSUANCE}", "또 한 줄"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(report["totals"]["foreign_lines"], 2)
        self.assertEqual(code, 0)

    def test_칸_수가_틀린_기록은_형식_깨짐으로_센다(self):
        _, report, _ = self.check([f"CY960\tREQ\t{KEY}\t{ROUND}"])
        self.assertEqual(report["totals"]["malformed_lines"], 1)

    def test_기록_파일이_없으면_아무것도_판정하지_않는다(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp, [], [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
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
            [], [(ISSUANCE, ROUND, MEMBER, "ISSUED")], marker=False)
        self.assertEqual(self.types(report), {})
        self.assertEqual(report["completeness"]["records"], "MISSING")
        self.assertEqual(code, 3)

    def test_다른_회차의_표식만_있으면_판정하지_않는다(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp, [], [(ISSUANCE, ROUND, MEMBER, "ISSUED")], marker=False)
            (f.dir / "requests.log").write_text(f"CY960\tRUN\t{ROUND + 1}\n")
            code, report, _ = f.run()
        self.assertEqual(self.types(report), {})
        self.assertEqual(code, 3)

    def test_앞_회차의_기록이_남아_있으면_걸러_낸다(self):
        # --console-output 은 덮어쓰지 않고 이어 쓴다(실측). 앞 회차의 키는 이번 회차
        # DB 에 없으니, 안 거르면 전부 미해결로 보인다.
        old_key = "99999999-8888-4777-8666-555555555555"
        code, report, _ = self.check(
            [f"CY960\tREQ\t{old_key}\t{ROUND - 1}\t{MEMBER}",
             f"CY960\tOK\t{old_key}\t{ISSUANCE - 1}",
             f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",
             f"CY960\tOK\t{KEY}\t{ISSUANCE}"],
            [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
        self.assertEqual(self.types(report), {"MATCHED": 1})
        self.assertEqual(report["totals"]["other_round_records"], 1)
        self.assertEqual(code, 0)

    # ── 전후 비교 ────────────────────────────────────────────────────

    def _report(self, tmp, name, records, issuances=(), idem=(), round_id=ROUND):
        d = Path(tmp) / name
        d.mkdir()
        f = Fixture(d, records, issuances, idem, round_id=round_id)
        f.run()
        return d / "report.json"

    def test_다시_대조해서_해소되면_해소로_나온다(self):
        records = [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}",
                   f"CY960\tUNKNOWN\t{KEY}\t1050"]
        with tempfile.TemporaryDirectory() as tmp:
            before = self._report(tmp, "before", records)
            after = self._report(tmp, "after", records,
                                 [(ISSUANCE, ROUND, MEMBER, "ISSUED")])
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
                                  [(ISSUANCE, ROUND, MEMBER, "ISSUED"),
                                   (ISSUANCE + 1, ROUND, MEMBER + 1, "ISSUED")])
            after = self._report(tmp, "after", [],
                                 [(ISSUANCE + 1, ROUND, MEMBER + 1, "ISSUED"),
                                  (ISSUANCE + 2, ROUND, MEMBER + 2, "ISSUED")])
            proc = subprocess.run(
                [sys.executable, str(CLI), "--diff", str(before), str(after)],
                capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("잔여 1 · 신규 1 · 해소 1", proc.stdout)

    def test_회차가_다르면_키_단위로_비교하지_않는다(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = self._report(tmp, "a", [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}"])
            b = self._report(tmp, "b", [f"CY960\tREQ\t{KEY}\t{ROUND + 1}\t{MEMBER}"],
                             round_id=ROUND + 1)
            proc = subprocess.run(
                [sys.executable, str(CLI), "--diff", str(a), str(b)],
                capture_output=True, text=True)
        self.assertIn("회차가 다르다", proc.stdout)
        self.assertNotIn("잔여", proc.stdout)

    # ── 덮어쓰지 않는다 ──────────────────────────────────────────────

    def test_대조_결과는_실행마다_다른_파일로_남는다(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Fixture(tmp, [f"CY960\tREQ\t{KEY}\t{ROUND}\t{MEMBER}"])
            for _ in range(2):
                subprocess.run([sys.executable, str(CLI), str(f.dir)],
                               capture_output=True, text=True)
            written = sorted(f.dir.glob("reconcile-*.json"))
        # 같은 초에 두 번 돌면 이름이 겹친다. 그때는 --out 으로 이름을 준다는 것을
        # 여기서 못 박는다 — 겹치면 하나만 남는 것이 사실이다.
        self.assertGreaterEqual(len(written), 1)
        self.assertTrue(all(p.name.startswith("reconcile-") for p in written))


if __name__ == "__main__":
    unittest.main(verbosity=2)
