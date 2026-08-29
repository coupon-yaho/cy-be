#!/usr/bin/env python3
"""회차 묶음을 읽어 반복분의 중앙값을 내고, 묶음 간 비교표를 만든다.

두 가지로 쓴다.

    summarize.py perf/results/<run-id>            한 묶음 요약
    summarize.py --compare <run-a> <run-b> ...    묶음 간 비교 (v1 회차 vs v2 회차)

규칙 둘을 코드로 박아 둔다.

  · 설정값과 달성치를 구분한다. configured_rate_per_sec 은 k6 에 넣은 도착률이고,
    achieved_rps 는 k6 결과의 perf.achieved_arrival_rps — 측정 구간에 실제로 쏜 요청을
    측정 구간 길이로 나눈 값이다. 표에서 두 열을 나란히 낸다.
    내장 http_reqs.rate 는 워밍업 시나리오까지 합산한 전체 실행 평균이라 달성치가
    아니다(실측으로 3배 이상 어긋났다). 비교용으로 http_reqs_rate_whole_run 에만 남긴다.
    돌아다니는 "1120/s" 는 어느 결과에도 없는 설정값이다.
  · 반복이 하나뿐이면 중앙값을 내지 않고 그렇게 표시한다. 표본 하나로 두 변형을
    비교하면 잡음과 차이를 구분할 수 없다.
"""
import json
import statistics
import sys
from pathlib import Path

TREND = ["med", "p(95)", "p(99)"]


def metric(summary, name, field):
    m = summary.get("metrics", {}).get(name)
    if not m:
        return None
    return m.get("values", {}).get(field)


def load_rep(d: Path):
    try:
        k6 = json.loads((d / "k6-summary.json").read_text())
        rnd = json.loads((d / "round.json").read_text())
        meta = json.loads((d / "meta.json").read_text())
    except (OSError, json.JSONDecodeError) as e:
        # 못 읽은 것과 0 은 다르다. 0 으로 채우지 않는다.
        print(f"  ! {d} 를 못 읽었다 — {e}", file=sys.stderr)
        return None
    # ⚠️ k6 가 0 이 아닌 코드로 끝난 반복은 온전한 측정이 아니다(중단·스크립트 오류).
    #    요약 파일이 남아 있다고 정상 반복으로 세면 그 값이 중앙값을 오염시킨다.
    if rnd.get("k6_exit_code", 0) != 0:
        print(f"  ! {d} 는 k6 rc={rnd['k6_exit_code']} 로 끝났다 — 중앙값에서 제외한다",
              file=sys.stderr)
        return None
    return {
        "dir": d.name,
        "configured_rps": rnd["configured_rate_per_sec"],
        "configured_requests": rnd["configured_requests"],
        "engine": rnd["engine"],
        # ⚠️ http_reqs.rate 가 아니다. 그건 워밍업 시나리오까지 합산한 전체 실행 평균이라
        #    측정 구간의 달성 도착률이 아니다(실측으로 3배 이상 어긋났다).
        "achieved_rps": k6.get("perf", {}).get("achieved_arrival_rps"),
        "http_reqs_rate_whole_run": metric(k6, "http_reqs", "rate"),
        "successes": metric(k6, "issue_successes", "count") or 0,
        "rejections": metric(k6, "issue_rejections", "count") or 0,
        "errors": metric(k6, "issue_errors", "count") or 0,
        "connect_failures": metric(k6, "issue_connect_failures", "count") or 0,
        "dropped": metric(k6, "dropped_iterations", "count") or 0,
        "success_med": metric(k6, "issue_success_duration", "med"),
        "success_p95": metric(k6, "issue_success_duration", "p(95)"),
        "success_p99": metric(k6, "issue_success_duration", "p(99)"),
        "soldout_med": metric(k6, "issue_rejected_sold_out_duration", "med"),
        "soldout_p99": metric(k6, "issue_rejected_sold_out_duration", "p(99)"),
        "over_issued": rnd["db_after"]["over_issued"],
        "dup_members": rnd["db_after"]["members_with_two_or_more"],
        "issuances": rnd["db_after"]["issuances"],
        "ping_avg_ms": meta["ping_b_to_a"]["avg_ms"],
        "ping_stddev_ms": meta["ping_b_to_a"]["stddev_ms"],
        "meta": meta,
    }


def med(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return statistics.median(vals)


def summarize_group(reps):
    keys = ["achieved_rps", "successes", "rejections", "errors", "connect_failures",
            "dropped", "success_med", "success_p95", "success_p99",
            "soldout_med", "soldout_p99", "issuances"]
    out = {"n": len(reps),
           "configured_rps": reps[0]["configured_rps"],
           "configured_requests": reps[0]["configured_requests"],
           "engine": reps[0]["engine"]}
    for k in keys:
        out[k] = med([r[k] for r in reps])
        out[k + "_all"] = [r[k] for r in reps]
    out["over_issued_any"] = any(r["over_issued"] for r in reps)
    out["dup_members_max"] = max(r["dup_members"] for r in reps)
    out["ping_avg_ms"] = med([r["ping_avg_ms"] for r in reps])
    out["ping_stddev_max"] = max((r["ping_stddev_ms"] or 0) for r in reps)
    return out


def load_run(run_dir: Path):
    groups = {}
    # 제외된 반복 수도 세어 둔다 — "원래 2회였다" 와 "5회 중 3회가 깨졌다" 는 다르다.
    for rate_dir in sorted(run_dir.glob("rate-*"), key=lambda p: int(p.name.split("-")[1])):
        found = sorted(rate_dir.glob("rep-*"))
        reps = [r for r in (load_rep(d) for d in found) if r]
        if reps:
            g = summarize_group(reps)
            g["excluded"] = len(found) - len(reps)
            groups[rate_dir.name] = g
    return groups


def fmt(v, digits=1):
    if v is None:
        return "측정실패"
    if isinstance(v, float):
        return f"{v:,.{digits}f}"
    return f"{v:,}"


def print_run(run_dir: Path, groups):
    print(f"\n== {run_dir.name}")
    any_meta = None
    for rate_dir in sorted(run_dir.glob("rate-*/rep-*")):
        p = rate_dir / "meta.json"
        if p.exists():
            any_meta = json.loads(p.read_text())
            break
    if any_meta:
        r, t, a, m = (any_meta["repo"], any_meta["topology"],
                      any_meta["api_declared"], any_meta["mysql"])
        print(f"   커밋 {r['commit'][:8]} ({r['branch']}) · 미커밋 {r['dirty_files']}건")
        print(f"   이미지 {any_meta['images']['api_tag']}")
        print(f"   api {t['running_api_containers']}대 · prometheus 타깃 {t['prometheus_api_targets']}개")
        print(f"   tomcat max-conn {a['tomcat_max_connections']} + accept {a['tomcat_accept_count']}"
              f" · threads {a['tomcat_threads_max']} · pool/instance {a['db_pool_size_per_instance']}")
        print(f"   heap {a['java_tool_options']} · mysql max_connections {m['max_connections']}")

    hdr = (f"{'구간':>10} {'n':>2} {'설정rps':>9} {'달성rps':>9} {'성공':>7} {'거절':>7} "
           f"{'5xx':>5} {'연결실패':>7} {'못쏨':>7} {'성공med':>9} {'성공p95':>9} {'성공p99':>9} {'매진p99':>9}")
    print("\n" + hdr)
    print("-" * len(hdr))
    for name, g in groups.items():
        n = g["n"]
        mark = "" if n >= 5 else f"  ← 반복 {n}회. 5회 미만은 중앙값으로 비교하지 않는다"
        if g.get("excluded"):
            mark += f"  [{g['excluded']}개 반복이 k6 비정상 종료로 제외됨]"
        print(f"{name:>10} {n:>2} {fmt(g['configured_rps'],0):>9} {fmt(g['achieved_rps']):>9} "
              f"{fmt(g['successes'],0):>7} {fmt(g['rejections'],0):>7} {fmt(g['errors'],0):>5} "
              f"{fmt(g['connect_failures'],0):>7} {fmt(g['dropped'],0):>7} "
              f"{fmt(g['success_med'],2):>9} {fmt(g['success_p95'],2):>9} {fmt(g['success_p99'],2):>9} "
              f"{fmt(g['soldout_p99'],2):>9}{mark}")

    print("\n   불변식")
    for name, g in groups.items():
        flags = []
        if g["over_issued_any"]:
            flags.append("초과발급")
        if g["dup_members_max"] > 0:
            flags.append(f"1인2매 {g['dup_members_max']}명")
        if (g["connect_failures"] or 0) > 0:
            flags.append(f"연결실패 {int(g['connect_failures'])}건 — 응답이 아니다. "
                         "톰캣 수용 상한과 클라이언트 임시 포트를 먼저 본다")
        print(f"     {name}: {'· '.join(flags) if flags else 'OK'}")
        print(f"        반복별 성공med = {[fmt(v,2) for v in g['success_med_all']]}")
        print(f"        ping avg {fmt(g['ping_avg_ms'],2)}ms · stddev 최대 {fmt(g['ping_stddev_max'],2)}ms")


def print_compare(runs):
    print("\n== 묶음 간 비교 (각 칸은 반복의 중앙값)")
    rates = sorted({r for _, g in runs for r in g}, key=lambda s: int(s.split("-")[1]))
    hdr = f"{'구간':>10} " + " ".join(f"{d.name[-16:]:>18}" for d, _ in runs)

    # ⚠️ 표본 수를 먼저 낸다. 이걸 안 보이면 1회짜리 묶음 둘을 나란히 놓은 표가
    #    정상 비교처럼 읽힌다 — 하네스의 5회 규약을 표가 우회하게 된다.
    print("\n  [반복 수 n]")
    print("  " + hdr)
    thin = []
    for rate in rates:
        cells = []
        for d, g in runs:
            if rate in g:
                n = g[rate]["n"]
                cells.append(str(n) if n >= 5 else f"{n} (부족)")
                if n < 5:
                    thin.append(f"{d.name}/{rate}={n}회")
            else:
                cells.append("—")
        print(f"  {rate:>10} " + " ".join(f"{c:>18}" for c in cells))
    if thin:
        print("\n  ⚠️ 5회 미만인 묶음이 있다 — " + ", ".join(thin) + ".")
        print("     같은 조건을 세 번 쟀을 때 med 가 3ms · 82ms · 224ms 로 흔들린 적이 있다.")
        print("     아래 표는 스모크 확인용이고, v1/v2 판정 근거로 인용하지 않는다.")
    for field, label, digits in [("achieved_rps", "달성rps", 1),
                                 ("success_med", "성공med(ms)", 2),
                                 ("success_p99", "성공p99(ms)", 2),
                                 ("soldout_p99", "매진p99(ms)", 2),
                                 ("connect_failures", "연결실패", 0)]:
        print(f"\n  [{label}]")
        print("  " + hdr)
        for rate in rates:
            cells = []
            for _, g in runs:
                cells.append(fmt(g[rate][field], digits) if rate in g else "—")
            print(f"  {rate:>10} " + " ".join(f"{c:>18}" for c in cells))
    print("\n  ⚠️ 두 묶음의 환경 메타가 다르면 이 표는 비교가 아니다. 각 묶음의 커밋·이미지·"
          "tomcat/풀 설정·ping 이 같은지 위에서 먼저 확인한다.")


def main(argv):
    if argv and argv[0] == "--compare":
        dirs = [Path(p) for p in argv[1:]]
        if len(dirs) < 2:
            sys.exit("--compare 에는 묶음 두 개 이상이 필요하다")
        runs = []
        for d in dirs:
            g = load_run(d)
            print_run(d, g)
            runs.append((d, g))
        print_compare(runs)
        return
    if not argv:
        sys.exit(__doc__)
    d = Path(argv[0])
    g = load_run(d)
    if not g:
        sys.exit(f"{d} 아래에 읽을 수 있는 회차가 없다")
    print_run(d, g)


if __name__ == "__main__":
    main(sys.argv[1:])
