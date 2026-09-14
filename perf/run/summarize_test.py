#!/usr/bin/env python3
"""요약표가 대조 결과를 어떻게 싣는지 태운다 (CY-965).

    python3 perf/run/summarize_test.py

**왜 이 시험이 있나** — 예전 요약표는 「불변식」에서 DB 안쪽 검사와 전송 오류만 보고
`OK` 를 찍었다. 바로 옆 `reconcile-*.json` 이 유실을 잡아 놓아도 `OK` 였고, 읽는
사람은 그것을 *"이 회차는 괜찮았다"* 로 읽는다. 그 `OK` 가 다시 대조를 덮지 않도록
못 박는다.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import summarize as S  # noqa: E402
from reconcile import SCHEMA  # noqa: E402


def report(counts=None, unjudged=()):
    """진짜 대조 보고서의 모양.

    **`schema` 를 빼면 안 된다.** `latest_report` 가 모양을 확인해서 아닌 것을
    판정 불가로 올리므로, 픽스처가 반쪽이면 걸러지는 것이 정상인 파일을 "보고서" 로
    놓고 시험하게 된다 — 실제로 그렇게 썼다가 시험 넷이 빨개졌다.
    """
    return {"schema": SCHEMA, "counts": counts or {}, "unjudged": list(unjudged),
            "generated_at": "2026-09-13T00:00:00+00:00"}


def reps(*reconciles):
    return [{"reconcile": r} for r in reconciles]


# meta.sh 는 이 칸들을 전부 채운다. 픽스처가 반쪽이면 print_run 이 못 도는,
# 런타임에 없는 모양을 시험하게 된다.
FULL_META = {
    "ping_b_to_a": {"avg_ms": 1.2, "stddev_ms": 0.3},
    "repo": {"commit": "abcdef1234567890", "branch": "main", "dirty_files": 0},
    "images": {"api_tag": "abcdef1"},
    "topology": {"running_api_containers": 4, "prometheus_api_targets": 4},
    "api_declared": {"tomcat_max_connections": 20000, "tomcat_accept_count": 20000,
                     "tomcat_threads_max": 15, "db_pool_size_per_instance": 3,
                     "java_tool_options": "-Xmx1g"},
    "mysql": {"max_connections": 50},
}


class ReconcileInSummaryTest(unittest.TestCase):

    # ── 접기 ─────────────────────────────────────────────────────────

    def test_유실은_중앙값이_아니라_합이다(self):
        # 5회 중 1회에서 3건이 빠졌으면 그것은 "중앙값 0" 이 아니라 "3건이 빠졌다" 다.
        folded = S.fold_reconcile(reps(report({"LOST": 3}), report(), report(),
                                       report(), report()))
        self.assertEqual(folded["counts"], {"LOST": 3})
        self.assertEqual((folded["reps"], folded["done"]), (5, 5))

    def test_여러_반복의_같은_유형은_더한다(self):
        folded = S.fold_reconcile(reps(report({"LOST": 2}), report({"LOST": 1, "ORPHAN": 4})))
        self.assertEqual(folded["counts"], {"LOST": 3, "ORPHAN": 4})

    def test_판정_불가_유형은_합집합이다(self):
        folded = S.fold_reconcile(reps(report(unjudged=["LOST"]),
                                       report(unjudged=["ORPHAN", "LOST"])))
        self.assertEqual(folded["unjudged"], ["LOST", "ORPHAN"])

    # ── 불변식 문구 ──────────────────────────────────────────────────

    def test_대조까지_깨끗하면_아무_말도_안_한다(self):
        # 이때만 「불변식」이 OK 를 찍을 수 있다.
        self.assertEqual(S.reconcile_flags(S.fold_reconcile(reps(report(), report()))), [])

    def test_대조를_안_했으면_OK_로_두지_않는다(self):
        # **침묵이 곧 OK 다.** 안 한 것과 했고 깨끗한 것을 가른다.
        flags = S.reconcile_flags(S.fold_reconcile(reps(None, None)))
        self.assertEqual(len(flags), 1)
        self.assertIn("대조 안 함", flags[0])
        self.assertIn("PERF_RECORD_REQUESTS", flags[0])   # 무엇을 해야 하는지도 적는다

    def test_결함이_있으면_유형과_수를_낸다(self):
        flags = S.reconcile_flags(S.fold_reconcile(reps(report({"LOST": 3, "ORPHAN": 1}))))
        self.assertEqual(len(flags), 1)
        self.assertIn("LOST 3", flags[0])
        self.assertIn("ORPHAN 1", flags[0])

    def test_두_번째_멱등_구간의_결함도_요약에_뜬다(self):
        """**자동으로 흐르는 것을 단정하지 않고 잰다.**

        `reconcile_flags` 는 `DEFECT` 멤버십으로 고른다. 그래서 CY-974 가 유형을
        더한 것만으로 요약에 뜨는 것이 맞는데, 그 연결이 끊기면 판정만 나고 요약은
        계속 초록이다 — CY-965 가 정확히 그 결함이었다.
        """
        flags = S.reconcile_flags(S.fold_reconcile(reps(report(
            {"NOTIFICATION_MISSING": 7, "OUTBOX_MISSING": 1,
             "NOTIFICATION_TARGET_MISMATCH": 1, "NOTIFICATION_ORPHAN": 2}))))
        self.assertEqual(len(flags), 1)
        for name, n in (("NOTIFICATION_MISSING", 7), ("OUTBOX_MISSING", 1),
                        ("NOTIFICATION_TARGET_MISMATCH", 1),
                        ("NOTIFICATION_ORPHAN", 2)):
            self.assertIn(f"{name} {n}", flags[0])

    def test_깨끗한_유형은_문구에_안_넣는다(self):
        # MATCHED 5,000 을 결함처럼 늘어놓으면 진짜 결함이 묻힌다.
        self.assertEqual(
            S.reconcile_flags(S.fold_reconcile(reps(report({"MATCHED": 5000})))), [])

    def test_보류는_결함과_따로_적고_다음_할_일을_말한다(self):
        flags = S.reconcile_flags(S.fold_reconcile(reps(report({"UNRESOLVED": 2}))))
        self.assertEqual(len(flags), 1)
        self.assertIn("UNRESOLVED 2", flags[0])
        self.assertIn("다시 대조", flags[0])

    def test_결함과_보류가_같이_있으면_둘_다_낸다(self):
        flags = S.reconcile_flags(
            S.fold_reconcile(reps(report({"LOST": 1, "UNRESOLVED": 2}))))
        self.assertEqual(len(flags), 2)
        self.assertTrue(any("LOST 1" in f for f in flags))
        self.assertTrue(any("UNRESOLVED 2" in f for f in flags))

    def test_일부만_대조했으면_그_사실을_먼저_적는다(self):
        # 3회의 결과가 5회의 결과로 읽히면 안 된다.
        flags = S.reconcile_flags(
            S.fold_reconcile(reps(report({"LOST": 1}), report(), report(), None, None)))
        self.assertIn("5회 중 3회만", flags[0])
        self.assertTrue(any("LOST 1" in f for f in flags))

    def test_판정_불가도_적는다(self):
        flags = S.reconcile_flags(S.fold_reconcile(reps(report(unjudged=["LOST", "ORPHAN"]))))
        self.assertTrue(any("판정 불가 2유형" in f for f in flags))

    def test_대조_칸_자체가_없으면_아무_말도_안_한다(self):
        # 반복이 전부 깨진 그룹이다. 그쪽은 "유효 반복 없음" 이 이미 말한다.
        self.assertEqual(S.reconcile_flags(None), [])
        self.assertEqual(S.reconcile_flags({"reps": 0, "done": 0, "counts": {},
                                            "unjudged": []}), [])

    def test_최신_보고서를_못_읽으면_안_함과_다르게_적는다(self):
        flags = S.reconcile_flags(S.fold_reconcile(
            reps({"unreadable": "reconcile-x.json — JSONDecodeError"})))
        self.assertTrue(any("못 읽음" in f for f in flags), flags)
        self.assertFalse(any("대조 안 함" in f for f in flags), flags)

    def test_못_읽은_보고서는_판정에_안_쓴다(self):
        folded = S.fold_reconcile(reps(report({"LOST": 2}), {"unreadable": "x"}))
        self.assertEqual(folded["counts"], {"LOST": 2})
        self.assertEqual((folded["reps"], folded["done"], folded["unreadable"]), (2, 1, 1))

    # ── 실제 출력 (여기가 이 티켓의 핵심이다) ────────────────────────

    def _run_dir(self, tmp, name, reconcile_counts, with_report=True):
        d = Path(tmp) / name / "rate-6667" / "rep-1"
        d.mkdir(parents=True)
        (d / "k6-summary.json").write_text(json.dumps(
            {"metrics": {}, "perf": {"achieved_arrival_rps": 6667.0}}))
        (d / "round.json").write_text(json.dumps(
            {"configured_rate_per_sec": 6667, "configured_requests": 20001,
             "engine": "V2", "k6_exit_code": 0,
             "db_after": {"over_issued": 0, "members_with_two_or_more": 0,
                          "issuances": 20000}}))
        (d / "meta.json").write_text(json.dumps(FULL_META))
        if with_report:
            (d / "reconcile-20260913T000000+0000.json").write_text(
                json.dumps(report(reconcile_counts)))
        return Path(tmp) / name

    def _dead_rep(self, d, counts):
        """k6 가 비정상 종료한 반복. 성능 표본에서는 빠지지만 대조 보고서는 남는다."""
        d.mkdir(parents=True)
        (d / "k6-summary.json").write_text(json.dumps({"metrics": {}, "perf": {}}))
        (d / "round.json").write_text(json.dumps(
            {"configured_rate_per_sec": 6667, "configured_requests": 20001,
             "engine": "V2", "k6_exit_code": 137,
             "db_after": {"over_issued": 0, "members_with_two_or_more": 0,
                          "issuances": 0}}))
        (d / "meta.json").write_text(json.dumps(FULL_META))
        (d / "reconcile-20260913T000000+0000.json").write_text(json.dumps(report(counts)))

    def invariant_line(self, run_dir):
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            S.print_run(run_dir, S.load_run(run_dir))
        lines = buf.getvalue().splitlines()
        i = next(n for n, l in enumerate(lines) if l.strip() == "불변식")
        return lines[i + 1]

    def test_대조가_유실을_잡으면_불변식이_OK_가_아니다(self):
        # **이 티켓이 고치는 자리다.** 예전에는 DB 안쪽 검사만 보고 OK 를 찍었고,
        # 바로 옆 보고서가 유실을 잡아 놓아도 요약은 OK 였다.
        with tempfile.TemporaryDirectory() as tmp:
            line = self.invariant_line(self._run_dir(tmp, "A", {"MATCHED": 19997, "LOST": 3}))
        self.assertIn("LOST 3", line)
        self.assertNotIn("OK", line)

    def test_대조를_안_했으면_불변식이_OK_가_아니다(self):
        with tempfile.TemporaryDirectory() as tmp:
            line = self.invariant_line(self._run_dir(tmp, "B", None, with_report=False))
        self.assertIn("대조 안 함", line)
        self.assertNotIn("OK", line)

    def test_k6_가_죽은_반복의_유실도_요약에_나온다(self):
        """**대조는 성능과 따로 접는다.**

        `load_rep` 은 k6 가 비정상 종료한 반복을 뺀다 — 그 반복의 분위수는 못 믿는다.
        그런데 `reconcile.sh` 는 그 반복도 돌아 보고서를 남긴다. 성능 표본에서 뺐다고
        유실까지 빼면 **죽은 회차의 결함이 통째로 사라진다** — 정작 그때가 유실이
        가장 잘 나는 자리다.
        """
        with tempfile.TemporaryDirectory() as tmp:
            run = self._run_dir(tmp, "D", {"MATCHED": 20000})       # rep-1: 정상
            self._dead_rep(run / "rate-6667" / "rep-2", {"LOST": 7})
            line = self.invariant_line(run)
        self.assertIn("LOST 7", line)
        self.assertNotIn("OK", line)

    def test_반복이_전부_죽어도_대조는_본다(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp) / "E"
            self._dead_rep(run / "rate-6667" / "rep-1", {"ORPHAN": 4})
            line = self.invariant_line(run)
        self.assertIn("ORPHAN 4", line)

    def test_대조까지_깨끗해야_OK_다(self):
        with tempfile.TemporaryDirectory() as tmp:
            line = self.invariant_line(self._run_dir(tmp, "C", {"MATCHED": 20000}))
        self.assertIn("OK", line)

    # ── 요약이 대조를 실행하지 않는다 ────────────────────────────────

    def test_DB_없이_보고서를_읽어_낸다(self):
        """**소스에서 "mysql" 을 찾지 않는다.**

        처음엔 그렇게 썼다가 `mysql max_connections` 라고 적힌 **주석에 걸렸다** —
        소스 텍스트로 계약을 단언하면 주석에 뚫리고 정상 코드를 오탐한다. 대신
        DB 도 회차 환경도 없는 디렉터리에서 실제로 읽어 내는지를 본다.
        """
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "k6-summary.json").write_text(json.dumps(
                {"metrics": {}, "perf": {"achieved_arrival_rps": 100.0}}))
            (d / "round.json").write_text(json.dumps(
                {"configured_rate_per_sec": 100, "configured_requests": 300,
                 "engine": "V2", "k6_exit_code": 0,
                 "db_after": {"over_issued": 0, "members_with_two_or_more": 0,
                              "issuances": 300}}))
            (d / "meta.json").write_text(json.dumps(
                {"ping_b_to_a": {"avg_ms": 1.0, "stddev_ms": 0.1}}))
            (d / "reconcile-20260913T000000+0000.json").write_text(
                json.dumps(report({"LOST": 2})))
            rep = S.load_rep(d)
        self.assertIsNotNone(rep)
        self.assertEqual(rep["reconcile"]["counts"], {"LOST": 2})

    def test_보고서가_없는_반복도_읽힌다(self):
        # 대조를 안 돌린 회차에서 요약이 죽으면 안 된다.
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "k6-summary.json").write_text(json.dumps({"metrics": {}, "perf": {}}))
            (d / "round.json").write_text(json.dumps(
                {"configured_rate_per_sec": 100, "configured_requests": 300,
                 "engine": "V2", "k6_exit_code": 0,
                 "db_after": {"over_issued": 0, "members_with_two_or_more": 0,
                              "issuances": 300}}))
            (d / "meta.json").write_text(json.dumps(
                {"ping_b_to_a": {"avg_ms": 1.0, "stddev_ms": 0.1}}))
            rep = S.load_rep(d)
        self.assertIsNotNone(rep)
        self.assertIsNone(rep["reconcile"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
