#!/usr/bin/env python3
"""부하 도구의 독립 기록과 DB 를 맞대 유실·고아·중복·불일치·미완료를 가른다 (CY-960).

    reconcile.py <반복 디렉터리>                  한 반복을 대조한다
    reconcile.py --diff <before.json> <after.json>  두 대조 결과를 비교한다

**왜 있나** — 부하 시험이 "몇 건 성공했나" 까지만 남기면 집합이 어긋난 것을 못 본다.
5,000건 받고 DB 에 5,000행이 있어도 그중 셋이 내가 못 받은 건이고 내가 받은 셋이
없을 수 있다. 합계는 맞고 집합은 다르다.

**무엇이 독립인가** — 커밋이 둘이라서 대조가 필요하다. 하나는 서버가 DB 에 넣은
커밋이고, 다른 하나는 **부하 도구가 요청을 보내기 전에 자기 파일에 적은 커밋**이다.
DB 안쪽끼리 맞대는 검증 배치는 이것을 못 잡는다 — *"클라이언트가 무엇을 받았는지"* 는
어느 테이블에도 없다.

**이 스크립트가 하지 않는 것**

  · DB 에 쓰지 않는다. 읽기 전용 SELECT 의 결과 파일만 읽는다
  · 주기 실행·서버·전용 화면을 만들지 않는다. 사람이 부를 때만 돈다
  · **추정으로 칸을 채우지 않는다.** 입력이 온전하지 않으면 그 유형을 판정하지 않고
    판정 불가로 남긴다 (종료코드 3)

종료코드
    0  결함 0 · 보류 0
    1  결함이 있다
    3  입력이 온전하지 않아 판정을 못 했다
    4  결함은 없고 보류(미완료·미해결)가 있다 — 나중에 다시 대조하라는 뜻
"""
import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = "cy960-reconcile/1"

# 독립 기록의 접두사. 같은 파일에 다른 console.* 이 섞여도 이 접두사로 골라낸다.
RECORD_PREFIX = "CY960"

# 발급이 있는 것이 정상인 유일한 거절이다. "이미 발급받은 쿠폰입니다" 라는 뜻이라
# 발급이 없으면 오히려 그쪽이 결함이다.
ALREADY_ISSUED_CODE = "COUPON-305"

# 발급 직후의 상태. 다른 값이면 대조 뒤에 무언가 더 일어난 것이다 (사용·취소·만료).
FRESH_ISSUANCE_STATUS = "ISSUED"

# 유형을 세 갈래로 가른다. 이 분류가 종료코드를 정한다.
CLEAN = (
    "MATCHED",                    # 성공 응답 ↔ 발급. 대상도 예약번호도 같다
    "MATCHED_STATUS_CHANGED",     # 발급은 있는데 ISSUED 가 아니다. 늦은 취소·사용
    "MATCHED_REJECTED",           # 명시적 거절 ↔ 발급 없음
    "ALREADY_ISSUED_CONFIRMED",   # 중복 거절 ↔ 발급 있음
    "RESOLVED_ISSUED",            # 결과 불명이었는데 DB 가 "접수됐다" 로 갈라 줬다
)
DEFECT = (
    "LOST",                  # 성공 응답을 받았는데 발급이 아무 데도 없다
    "KEY_MISMATCH",          # 그 대상에 발급은 있는데 **내 접수 키로 만들어지지 않았다**
    "TARGET_MISMATCH",       # 내 접수 키의 발급인데 **대상(회차·회원)이 다르다**
    "MISMATCH",              # 받은 예약번호가 DB 와 다르거나 못 읽었다
    "CONTENT_MISMATCH",      # **요청한 내용과 저장된 내용이 다르다** — 원인이 다르다
    "FALSE_REJECT",          # 안 받았다고 해 놓고 내 키의 발급이 있다
    "ALREADY_ISSUED_PHANTOM",# 이미 있다고 거절해 놓고 그 대상에 발급이 없다
    "ORPHAN",                # 발급의 접수 키가 보낸 기록에 없다
    "DUPLICATE",             # **한 접수 키가 발급 둘을 만들었다** — 멱등이 깨졌다
    "DUPLICATE_TARGET",      # 한 대상에 발급이 둘 이상 — uk_coupon_member 가 깨졌다
    "UNATTRIBUTABLE",        # 이 발급을 어느 접수 키에 붙일지 정할 수 없다
    "DANGLING_IDEM",         # 멱등이 완료라는데 그 발급이 없다
)
PENDING = (
    "INCOMPLETE",   # 서버가 접수 키를 잡아만 두고 결론을 못 냈다 (IN_PROGRESS)
    "UNRESOLVED",   # 결과 불명이고 DB 에도 흔적이 없다. 아직 못 가른다
)
ALL_TYPES = CLEAN + DEFECT + PENDING

# 판정에 무엇이 필요한가. 그 입력이 불완전하면 그 유형은 판정하지 않는다.
#
# ⚠️ "대상 0이면 판정 불가" 로 뭉뚱그리지 않는다. 발급 덤프만 깨졌으면 멱등 쪽 판정은
#    여전히 유효하고, 그것까지 버리면 진짜 검출을 가드가 덮는다.
NEEDS = {
    "issuances": tuple(t for t in ALL_TYPES if t != "INCOMPLETE"),
    # 내용을 안 담은 기록(옛 형식)으로는 이 축을 판정할 수 없다. 없는 값을 "맞다" 로
    # 세는 것이 이 도구가 가장 하면 안 되는 일이다.
    "content": ("CONTENT_MISMATCH",),
    # 접수 키가 조인 축이다. 이력을 못 읽으면 "내 키의 발급" 을 아예 못 고른다.
    "histories": ("MATCHED", "MATCHED_STATUS_CHANGED", "LOST", "KEY_MISMATCH",
                  "TARGET_MISMATCH", "MISMATCH", "FALSE_REJECT", "RESOLVED_ISSUED",
                  "ORPHAN", "DUPLICATE", "UNATTRIBUTABLE", "UNRESOLVED"),
    "idempotency": ("INCOMPLETE", "UNRESOLVED", "DANGLING_IDEM"),
    "records": ALL_TYPES,
}


class Incomplete(Exception):
    """입력을 온전하게 못 읽었다. 0 으로 채우지 않고 판정 불가로 올린다."""


# ─────────────────────────────── 입력 읽기 ───────────────────────────────

def read_tsv(path: Path, columns: int):
    """`#EOF <행수>` 로 끝나는 TSV 를 읽는다.

    센티널이 없거나 행수가 어긋나면 예외다. **잘린 파일을 "0행" 으로 읽으면 그 순간
    전량 유실로 보인다** — 덤프가 죽은 것과 DB 가 빈 것은 다르다.
    """
    if not path.exists():
        raise Incomplete(f"{path.name} 이 없다")
    lines = path.read_text().splitlines()
    if not lines or not lines[-1].startswith("#EOF"):
        raise Incomplete(f"{path.name} 에 #EOF 센티널이 없다 — 덤프가 중간에 끊겼다")
    declared = lines[-1].split("\t")
    if len(declared) != 2 or not declared[1].isdigit():
        raise Incomplete(f"{path.name} 의 #EOF 행이 `#EOF<탭><행수>` 가 아니다")
    rows = []
    for n, line in enumerate(lines[:-1], start=1):
        parts = line.split("\t")
        if len(parts) != columns:
            raise Incomplete(f"{path.name}:{n} 이 {columns}칸이 아니라 {len(parts)}칸이다")
        rows.append(parts)
    if len(rows) != int(declared[1]):
        raise Incomplete(
            f"{path.name} 이 {int(declared[1])}행이라 했는데 {len(rows)}행이다")
    return rows


def read_records(path: Path):
    """독립 기록을 접수 키 단위로 묶고, 어느 회차의 기록인지를 함께 낸다.

    **같은 접수 키의 재전송은 한 신청이다.** 멱등 키를 붙이는 이유가 그것이라, 여기서
    묶지 않으면 재시도 한 번이 곧바로 중복으로 보인다.
    """
    if not path.exists():
        raise Incomplete(f"{path.name} 이 없다 — PERF_RECORD_REQUESTS=true 로 돌렸나")
    by_key, runs = {}, set()
    stats = {"lines": 0, "records": 0, "foreign": 0, "malformed": 0}
    for line in path.read_text().splitlines():
        stats["lines"] += 1
        parts = line.split("\t")
        if parts[0] != RECORD_PREFIX:
            stats["foreign"] += 1          # 다른 console.* 한 줄. 세되 판정에 안 쓴다
            continue
        stats["records"] += 1
        kind = parts[1] if len(parts) > 1 else ""
        if kind == "RUN" and len(parts) == 3:
            runs.add(parts[2])
        elif kind == "REQ" and len(parts) in (5, 6):
            # 5칸은 내용을 안 담던 옛 형식이다. **거절하지 않는다** — 옛 회차를 다시
            # 대조하는 것이 이 도구의 쓰임새이고, 거절하면 그 회차가 통째로 판정
            # 불가가 된다. 대신 내용 축만 판정하지 않는다(아래 completeness["content"]).
            key, rnd, member = parts[2], parts[3], parts[4]
            entry = by_key.setdefault(key, _new_entry())
            entry["attempts"] += 1
            entry["targets"].add((rnd, member))
            if len(parts) == 6:
                entry["contents"].add(parts[5])
            else:
                entry["contents"].add(None)
        elif kind in ("OK", "REJECTED", "UNKNOWN") and len(parts) == 4:
            by_key.setdefault(parts[2], _new_entry())["outcomes"].append(
                (kind, parts[3]))
        else:
            stats["malformed"] += 1
    return by_key, runs, stats


def _new_entry():
    return {"attempts": 0, "targets": set(), "contents": set(), "outcomes": []}


# ─────────────────────────────── 판정 ───────────────────────────────

def wrong_content(entry, row):
    """요청한 내용과 저장된 내용이 다른가.

    ⚠️ **내용을 모르면 반드시 `False` 를 내야 한다.** 이 함수는 `elif` 사슬 안에
    있어서, 참을 내면 그 갈래가 기록을 가져간다. 그런데 `CONTENT_MISMATCH` 가
    `unjudged` 면 `add` 가 아무것도 안 남기고 — **그 기록은 어떤 판정에도 안 잡힌다.**
    `MATCHED` 로 갈 기회 자체가 사라진다.

    한 번 이 가드를 "바깥 게이트가 이미 거르니 죽은 코드" 로 보고 지웠다가 시험이
    잡았다. 돌연변이 하나가 살아남은 것을 **도달 불가**로 읽었는데, 실은 그 돌연변이가
    다른 경로로 같은 결과를 냈을 뿐이었다.

    한 키에 내용이 여럿인 경우는 불일치다. 같은 키에 다른 내용을 보낸 것이고,
    서버는 그것을 `COUPON-404` 로 거절한다.
    """
    if not entry["contents"] or None in entry["contents"]:
        return False
    return entry["contents"] != {row["content"]}


def conclude(outcomes):
    """한 접수 키의 결론. **가장 확정적인 결과가 이긴다.**

    성공이 한 번이라도 있으면 접수된 것이다 — 뒤이은 재시도가 "이미 있다" 로 거절돼도
    신청은 받아들여졌다. 거절은 서버가 안 받았다고 **말한** 것이라 결과 불명보다 세다.
    """
    kinds = {k for k, _ in outcomes}
    for kind in ("OK", "REJECTED", "UNKNOWN"):
        if kind in kinds:
            return kind, [v for k, v in outcomes if k == kind]
    return "NO_RESPONSE", []          # REQ 만 있고 결과 줄이 없다. 이것도 결과 불명이다


def judge(records, issuances, histories, idem, target_round, judged):
    """접수 키마다 하나, 그리고 DB 쪽에서 짝이 없는 것마다 하나씩 판정을 낸다.

    **축이 둘이다.** 접수 키는 *"내 신청이 이 발급이 됐나"* 를 답하고, 대상(회차·회원)은
    *"그 회원에게 이미 있나"* 를 답한다. 둘은 다른 질문이라 한쪽으로 갈음할 수 없다.

    대상만으로 조인하면 **대상 오류를 낼 수가 없다** — 대상으로 찾았으니 찾힌 행의
    대상은 언제나 맞다. 키만으로 조인하면 *"이미 발급받았다"* 는 거절을 확인할 수
    없다 — 그 발급은 **다른 접수 키**가 만든 것이다.
    """
    findings = []
    by_target = defaultdict(list)
    for row in issuances:
        by_target[(row["coupon_id"], row["member_id"])].append(row)
    by_id = {row["id"]: row for row in issuances}

    # 접수 키 ↔ 발급. 이력이 없는 발급과 키가 갈리는 발급을 함께 가려낸다.
    mine_of, keys_of = defaultdict(list), defaultdict(set)
    if histories is not None:
        for issuance_id, request_id in histories:
            keys_of[issuance_id].add(request_id)
        for issuance_id, keys in keys_of.items():
            if len(keys) == 1 and (only := next(iter(keys))):
                mine_of[only].append(issuance_id)
    claimed = set()

    def add(vtype, key, detail, **extra):
        if vtype in judged:
            findings.append({"type": vtype, "key": key, "detail": detail, **extra})

    for key, entry in sorted(records.items()):
        if len(entry["targets"]) != 1:
            add("MISMATCH", key,
                f"한 접수 키에 대상이 {len(entry['targets'])}가지다")
            continue
        rnd, member = next(iter(entry["targets"]))
        kind, values = conclude(entry["outcomes"])
        common = {"round": rnd, "member": member, "attempts": entry["attempts"]}

        # ① 내 접수 키가 만든 발급. ② 그 대상에 있는 발급. 서로 다른 질문이다.
        mine = [by_id[i] for i in mine_of.get(key, []) if i in by_id]
        at_target = by_target.get((rnd, member), [])
        claimed.update(r["id"] for r in mine)

        if len(mine) > 1:
            add("DUPLICATE", key,
                f"한 접수 키가 발급 {[r['id'] for r in mine]} 를 만들었다 — 멱등이 깨졌다",
                **common)
            continue
        row = mine[0] if mine else None

        # ── 대상·내용은 **결과와 무관하게** 본다 ────────────────────────────
        #
        # 내 접수 키의 발급이 잡혔으면, 그것이 내가 요청한 대상·내용인지는 응답을
        # 받았든 잃었든 똑같이 물어야 하는 질문이다. 예전에는 이 둘이 `OK` 갈래
        # 안에만 있어서, **응답을 잃은 건은 내용이 달라도 `RESOLVED_ISSUED`(정상)로
        # 나갔다** — 실측으로 재현했다(VIP 요청 · BASIC 저장 · 종료코드 0).
        #
        # ⚠️ `wrong_content` 는 내용을 모르면 거짓을 낸다. 그래야 그 유형이
        #    `unjudged` 일 때 이 `continue` 가 기록을 통째로 삼키지 않는다.
        if row is not None and (row["coupon_id"], row["member_id"]) != (rnd, member):
            add("TARGET_MISMATCH", key,
                f"내 접수 키의 발급 {row['id']} 이"
                f" 회차 {row['coupon_id']} · 회원 {row['member_id']} 앞으로 있다",
                **common)
            continue
        if row is not None and wrong_content(entry, row):
            # **대상은 맞는데 내용이 다르다.** MISMATCH 와 원인이 다르다 —
            # 그쪽은 응답과 저장의 어긋남이고 이쪽은 **요청과 저장**의 어긋남이다.
            add("CONTENT_MISMATCH", key,
                f"요청한 내용 {sorted(entry['contents'])} · DB 의 발급"
                f" {row['id']} 은 {row['content']}", **common)
            continue

        if kind == "OK":
            if row is None and not at_target:
                got = ", ".join(v for v in values if v) or "예약번호 없음"
                add("LOST", key, f"성공 응답({got})을 받았는데 발급이 없다", **common)
            elif row is None:
                # **대상만 보면 정상으로 보이는 자리다.** 발급은 있는데 내 접수 키가
                # 아닌 다른 키로 만들어졌다. 그 발급은 아래 고아 검사에도 걸린다.
                add("KEY_MISMATCH", key,
                    f"발급 {[r['id'] for r in at_target]} 이 그 대상에 있는데"
                    " 내 접수 키로 만들어진 것이 아니다", **common)
            elif len({v for v in values if v}) > 1:
                # 재전송이 서로 다른 예약번호를 받았다. 멱등이 깨졌다는 뜻이고,
                # DB 에 한 행뿐이어도 그렇다 — 클라이언트가 받은 것이 증거다.
                add("MISMATCH", key,
                    f"한 접수 키에 예약번호가 여럿이다 — {sorted(set(values))}", **common)
            elif not any(values):
                add("MISMATCH", key,
                    f"성공 응답에서 예약번호를 못 읽었다 · DB 의 발급 {row['id']}",
                    **common)
            elif values[0] and values[0] != row["id"]:
                add("MISMATCH", key,
                    f"받은 예약번호 {values[0]} · DB 의 발급 {row['id']}", **common)
            elif row["status"] != FRESH_ISSUANCE_STATUS:
                add("MATCHED_STATUS_CHANGED", key,
                    f"발급 {row['id']} 이 {row['status']} 다", **common)
            else:
                add("MATCHED", key, f"발급 {row['id']}", **common)
        elif kind == "REJECTED":
            # 거절이 여럿이면 **"이미 있다" 가 이긴다.** 그 거절만이 발급이 있다는
            # 증언이라, 매진 거절을 먼저 집으면 멀쩡한 발급이 거짓 거절로 잡힌다.
            code = (ALREADY_ISSUED_CODE if ALREADY_ISSUED_CODE in values
                    else values[0])
            if row is not None:
                # 안 받았다면서 **내 키의** 발급이 생겼다. 커밋해 놓고 응답에서 터진 꼴이다.
                add("FALSE_REJECT", key,
                    f"{code} 로 거절했는데 내 접수 키의 발급 {row['id']} 이 있다", **common)
            elif code == ALREADY_ISSUED_CODE:
                # **이것은 대상에 대한 주장이다.** 그 발급은 다른 접수 키가 만들었으니
                # 키로 찾으면 안 나오는 것이 정상이다.
                if at_target:
                    add("ALREADY_ISSUED_CONFIRMED", key,
                        f"발급 {[r['id'] for r in at_target]}", **common)
                else:
                    add("ALREADY_ISSUED_PHANTOM", key,
                        "이미 있다고 거절했는데 그 대상에 발급이 없다", **common)
            else:
                # 대상에 남의 키로 만든 발급이 있어도 여기서 세지 않는다.
                # 그 행은 고아로 한 번만 보고한다 — 같은 행을 두 번 세면 수가 부푼다.
                add("MATCHED_REJECTED", key, code, **common)
        else:                                   # UNKNOWN · NO_RESPONSE
            reason = values[0] if values else "응답 줄 없음"
            if row is not None:
                add("RESOLVED_ISSUED", key,
                    f"결과 불명({reason})이었는데 내 접수 키의 발급 {row['id']} 이 있다",
                    **common)
            elif idem is not None and key in idem \
                    and idem[key]["status"] != "DONE":
                add("INCOMPLETE", key,
                    f"결과 불명({reason}) · 멱등이 {idem[key]['status']} 로 멈춰 있다",
                    **common)
            else:
                add("UNRESOLVED", key,
                    f"결과 불명({reason}) · 내 접수 키의 발급이 없다", **common)

    # ── DB 쪽에서 짝이 없는 것 ────────────────────────────────────────
    for (coupon, member), rows in sorted(by_target.items()):
        if coupon != target_round or len(rows) <= 1:
            continue
        # 한 접수 키가 만든 중복이면 DUPLICATE 가 이미 보고했다. **같은 행을 두 번
        # 세지 않는다** — 수가 부풀면 다음 사람이 결함을 두 배로 읽는다.
        # 여기가 더하는 정보는 "서로 다른 키가 같은 대상에 발급을 만들었다" 뿐이다.
        seen = {k for r in rows for k in keys_of.get(r["id"], {""})}
        if histories is not None and len(seen) == 1:
            continue
        add("DUPLICATE_TARGET", f"대상:{coupon}/{member}",
            f"한 대상에 서로 다른 접수 키의 발급이 {len(rows)}행"
            f" — {[r['id'] for r in rows]}",
            round=coupon, member=member)

    if histories is not None:
        for row in sorted(issuances, key=lambda r: r["id"]):
            if row["coupon_id"] != target_round or row["id"] in claimed:
                continue
            keys = {k for k in keys_of.get(row["id"], set()) if k}
            if len(keys) != 1:
                add("UNATTRIBUTABLE", f"발급:{row['id']}",
                    "ISSUE 이력이 없다" if not keys
                    else f"ISSUE 이력의 접수 키가 {len(keys)}가지다",
                    round=row["coupon_id"], member=row["member_id"])
            else:
                # 고아의 이름은 **그 발급이 달고 있는 접수 키**다. 대상으로 이름
                # 붙이면 같은 대상의 두 고아가 전후 비교에서 한 원소로 뭉개진다.
                add("ORPHAN", next(iter(keys)),
                    f"발급 {row['id']} 의 접수 키가 보낸 기록에 없다",
                    round=row["coupon_id"], member=row["member_id"])

    if idem is not None:
        for key, rec in sorted(idem.items()):
            if rec["status"] == "DONE" and rec["issuance_id"] \
                    and rec["issuance_id"] not in by_id:
                add("DANGLING_IDEM", key,
                    f"멱등이 완료라는데 발급 {rec['issuance_id']} 이 대상 회차에 없다")
    return findings


# ─────────────────────────────── 실행 ───────────────────────────────

def reconcile(rep: Path):
    completeness, problems = {}, []
    rnd = json.loads((rep / "round.json").read_text())
    target_round = str(rnd["target_round_id"])
    other_round = 0
    try:
        records, runs, stats = read_records(rep / "requests.log")
        # **빈 파일을 "요청 0건" 으로 읽지 않는다.** k6 는 기록을 꺼도
        # --console-output 파일을 만들고, 그것을 0건으로 읽으면 이 회차의 발급이
        # 전부 고아로 보고된다 — 멀쩡한 회차에서 만 건짜리 거짓 결함이 난다.
        if target_round not in runs:
            raise Incomplete(
                f"requests.log 에 회차 {target_round} 의 기록 시작 표식이 없다"
                " — PERF_RECORD_REQUESTS=true 로 돌린 회차가 맞나")
        # --console-output 은 이어 쓴다(실측). 같은 디렉터리에 두 번 쏘면 앞 회차의
        # 기록이 그대로 남아 있고, 그 키들은 이번 회차 DB 에 없어 전부 미해결로 보인다.
        keep = {}
        for key, entry in records.items():
            if entry["targets"] and all(t[0] != target_round for t in entry["targets"]):
                other_round += 1
            else:
                keep[key] = entry
        records = keep
        completeness["records"] = "COMPLETE"
        # 한 건이라도 내용이 없으면 그 축은 판정하지 않는다. 옛 형식 기록이 섞이면
        # **없는 값을 "맞다" 로 세게 되고**, 그것이 이 도구가 가장 하면 안 되는 일이다.
        # 기록이 하나도 없으면 이 축은 **할 말이 없는 것**이지 못 재는 것이 아니다.
        # 그때까지 판정 불가로 만들면 멀쩡한 회차가 빨개진다.
        completeness["content"] = (
            "COMPLETE" if all(None not in e["contents"] for e in records.values())
            else "MISSING")
    except Incomplete as e:
        records, stats = {}, {"lines": 0, "records": 0, "foreign": 0, "malformed": 0}
        completeness["records"] = "MISSING"
        completeness["content"] = "MISSING"
        problems.append(str(e))

    try:
        issuances = [{"id": r[0], "coupon_id": r[1], "member_id": r[2],
                      "status": r[3], "content": r[4]}
                     for r in read_tsv(rep / "db-issuances.tsv", 5)]
        completeness["issuances"] = "COMPLETE"
    except Incomplete as e:
        issuances = []
        completeness["issuances"] = "PARTIAL"
        problems.append(str(e))

    try:
        idem = {r[0]: {"status": r[1], "member_id": r[2], "issuance_id": r[3]}
                for r in read_tsv(rep / "db-idempotency.tsv", 4)}
        completeness["idempotency"] = "COMPLETE"
    except Incomplete as e:
        idem = None
        completeness["idempotency"] = "PARTIAL"
        problems.append(str(e))

    try:
        # (발급 id, 접수 키). 조인 축이라 못 읽으면 대부분의 판정이 멈춘다.
        histories = [(r[0], r[1]) for r in read_tsv(rep / "db-histories.tsv", 2)]
        completeness["histories"] = "COMPLETE"
    except Incomplete as e:
        histories = None
        completeness["histories"] = "PARTIAL"
        problems.append(str(e))

    # **기록기 자체를 잰다.** measure_attempts 는 k6 가 프로세스 안에서 센 측정 시도
    # 수이고, REQ 줄 수와 **같아야 한다** — 둘 다 measure() 한 번에 하나씩 는다.
    dropped, attempts, retries = 0, None, 0
    summary = rep / "k6-summary.json"
    if summary.exists():
        k6 = json.loads(summary.read_text())
        dropped = (k6.get("metrics", {})
                   .get("dropped_iterations", {}).get("values", {}).get("count", 0))
        attempts = k6.get("perf", {}).get("measure_attempts")
        retries = k6.get("perf", {}).get("measure_retries") or 0
    recorded = sum(e["attempts"] for e in records.values())
    applications = len(records)
    intact = completeness["records"] == "COMPLETE"
    # **두 축을 다 본다.** 하나로는 못 잡는 것이 서로 다르다 — 같은 입력으로 견줘 봤다.
    #
    #   줄 == 시도 + 재전송   중복 기록·재전송 줄 유실을 잡는다
    #   키 == 시도            **재전송이 새 키를 쓴 것**을 잡는다 (위 검사는 통과한다)
    #
    # 뒤엣것이 특히 중요하다. 재전송은 **같은 접수 키**여야 하고, 새 키를 쓰면 서버가
    # 그것을 새 신청으로 보아 한 사람이 둘을 받는다. 그런데 줄 수는 그대로 맞는다.
    if intact and attempts is not None and recorded != attempts + retries:
        problems.append(
            f"k6 는 시도 {attempts} · 재전송 {retries} 로 셌는데 기록은 {recorded}줄이다"
            " — 기록이 온전하지 않다")
        intact = False
    if intact and attempts is not None and applications != attempts:
        problems.append(
            f"k6 는 시도를 {attempts}건으로 셌는데 서로 다른 접수 키가 {applications}개다"
            " — 재전송이 같은 키를 안 썼을 수 있다")
        intact = False
    # 형식이 깨진 줄은 **시도 수로는 안 잡힌다.** REQ 는 멀쩡한데 결과 줄 하나가
    # 깨지면 시도 수가 그대로 맞고, 그 건은 조용히 "결과 불명" 이 된다 — 성공을 받은
    # 건이 유실(결함)이 아니라 미해결(보류)로 내려간다. 실측으로 확인했다.
    if intact and stats["malformed"]:
        problems.append(
            f"형식이 깨진 기록 줄이 {stats['malformed']}줄이다 — 그 줄이 무엇이었는지"
            " 알 수 없다. 결과 줄이 깨졌으면 성공한 건이 결과 불명으로 보인다")
        intact = False
    if not intact and completeness["records"] == "COMPLETE":
        # **적기만 하고 넘어가지 않는다.** 기록이 온전치 않으면 빠지거나 깨진 키가
        # 애초에 없던 것처럼 보여, 결함이 보류로 내려가거나 아예 안 보인다. 잘린
        # 덤프를 빈 덤프로 읽지 않는 것과 같은 이유다. 몇 줄까지 봐 줄지는 안 재
        # 봤고, **안 잰 임계값을 박는 대신 어긋나면 판정하지 않는다.**
        completeness["records"] = "PARTIAL"
        completeness["content"] = "MISSING"
        records = {}

    judged = {t for t in ALL_TYPES
              if all(t not in types or completeness[src] == "COMPLETE"
                     for src, types in NEEDS.items())}
    findings = judge(records, issuances, histories, idem, target_round, judged)

    counts = {t: 0 for t in sorted(judged)}
    for f in findings:
        counts[f["type"]] += 1

    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "input": {"dir": str(rep), "engine": rnd["engine"],
                  "target_round_id": rnd["target_round_id"]},
        "completeness": completeness,
        "unjudged": sorted(set(ALL_TYPES) - judged),
        "problems": problems,
        "totals": {
            # 보낸 기록조차 없는 요청이 있으면 **양쪽에서 동시에 빠진 것**이다. 그건 여기
            # 총계로만 보인다 — 어느 키인지는 알 방법이 없다.
            "configured_requests": rnd["configured_requests"],
            "recorded_requests": recorded,
            "recorded_applications": applications,
            "k6_measure_attempts": attempts,
            "k6_measure_retries": retries,
            "dropped_iterations": int(dropped),
            "record_lines": stats["lines"],
            "foreign_lines": stats["foreign"],
            "malformed_lines": stats["malformed"],
            "other_round_records": other_round,
        },
        "counts": counts,
        "findings": findings,
    }


def verdict(report):
    """종료코드와 한 줄 판정.

    **확정된 결함이 판정 불가보다 세다.** 못 잰 칸이 있다고 이미 잰 결함을 덮으면,
    부르는 쪽이 "다시 돌려 보자" 로 읽고 그 결함을 한 번 더 지나친다.
    """
    counts = report["counts"]
    labels = []
    if any(counts.get(t) for t in DEFECT):
        labels.append("결함")
    if report["unjudged"]:
        labels.append("일부 판정 불가")
    if any(counts.get(t) for t in PENDING):
        labels.append("보류")
    if not labels:
        return 0, "정상"
    if "결함" in labels:
        return 1, " · ".join(labels)
    if report["unjudged"]:
        return 3, " · ".join(labels)
    return 4, " · ".join(labels)


def print_report(report):
    code, label = verdict(report)
    t = report["totals"]
    print(f"대조 — 회차 {report['input']['target_round_id']} · {report['input']['engine']}")
    print(f"  신청 {t['recorded_applications']} / 설정 {t['configured_requests']} "
          f"(못 쏨 {t['dropped_iterations']} · 재전송 {t['k6_measure_retries']}"
          f" · 기록 {t['recorded_requests']}줄)")
    # ⚠️ **줄 수와 맞대면 안 된다.** 재전송이 있으면 줄이 신청보다 많아져 이 수가
    #    음수로 나온다(실측: 설정 11 · 줄 16 → -5). 설정 요청 수가 세는 것은
    #    **신청**이지 보낸 횟수가 아니다.
    unrecorded = (t["configured_requests"] - t["recorded_applications"]
                  - t["dropped_iterations"])
    # **부호를 갈라 읽는다.** 모자란 것과 남는 것은 정반대의 사실이다.
    # 남는 쪽은 재전송이 새 키를 쓴 경우에 난다 — 그때 "보낸 기록이 없다" 고 적으면
    # 문구가 사실과 거꾸로다(기록은 오히려 더 많다). 실측으로 -1 을 봤다.
    if unrecorded > 0:
        print(f"  ⚠️ 설명 안 되는 {unrecorded}건 — 보낸 기록도 없고 못 쐈다는 기록도 없다")
    elif unrecorded < 0:
        print(f"  ⚠️ 설정보다 신청이 {-unrecorded}건 많다 —"
              " 재전송이 새 접수 키를 썼을 수 있다")
    if t["foreign_lines"] or t["malformed_lines"] or t["other_round_records"]:
        print(f"  ⚠️ 남의 줄 {t['foreign_lines']} · 형식 깨진 줄 {t['malformed_lines']}"
              f" · 다른 회차의 기록 {t['other_round_records']}")
    for problem in report["problems"]:
        print(f"  ! {problem}", file=sys.stderr)
    if report["unjudged"]:
        print(f"  ! 판정 못 한 유형 — {', '.join(report['unjudged'])}", file=sys.stderr)
    for name, n in report["counts"].items():
        if n:
            mark = "✗" if name in DEFECT else ("?" if name in PENDING else "·")
            print(f"  {mark} {name:24s} {n}")
    print(f"  ⇒ {label}")
    return code


def diff(before, after):
    """같은 회차를 두 시점에 대조한 결과를 키·유형별로 가른다.

    **회차가 다르면 키 단위로 비교하지 않는다.** 접수 키가 (회차, 회원) 에서 나오므로
    회차가 다르면 키 집합이 통째로 다르고, 그러면 모든 항목이 "해소 + 신규" 로 나와
    아무 뜻이 없다.
    """
    same = before["input"]["target_round_id"] == after["input"]["target_round_id"]
    # 한쪽에서 판정하지 못한 유형은 **비교할 수 없다.** 그쪽 목록이 비어 있는 것은
    # "없다" 가 아니라 "모른다" 인데, 집합으로 빼면 전부 신규나 해소로 나온다 —
    # 아무 일도 안 일어났는데 변화가 있었던 것처럼 보인다.
    unjudged = set(before.get("unjudged", [])) | set(after.get("unjudged", []))
    out = {"schema": SCHEMA, "per_key": same,
           "before": before["generated_at"], "after": after["generated_at"],
           "not_comparable": sorted(unjudged), "by_type": {}}
    types = sorted(set(before["counts"]) | set(after["counts"]) | unjudged)
    for t in types:
        entry = {"before": before["counts"].get(t), "after": after["counts"].get(t)}
        if same and t not in unjudged:
            b = {f["key"] for f in before["findings"] if f["type"] == t}
            a = {f["key"] for f in after["findings"] if f["type"] == t}
            entry.update(comparable=True, residual=sorted(b & a), new=sorted(a - b),
                         cleared=sorted(b - a))
        else:
            entry["comparable"] = False
        out["by_type"][t] = entry
    return out


def print_diff(out):
    cleared = 0
    if not out["per_key"]:
        print("⚠️ 회차가 다르다. 유형별 집계만 비교한다 — 키 단위 비교는 뜻이 없다")
    print(f"{out['before']}  →  {out['after']}")
    for t, e in out["by_type"].items():
        blank = not e["before"] and not e["after"]
        if blank and t not in out["not_comparable"]:
            continue
        line = f"  {t:24s} {e['before']} → {e['after']}"
        if e["comparable"]:
            line += (f"   잔여 {len(e['residual'])} ·"
                     f" 신규 {len(e['new'])} · 해소 {len(e['cleared'])}")
            cleared += len(e["cleared"])
        elif t in out["not_comparable"]:
            line += "   비교 불가 — 한쪽이 이 유형을 판정하지 못했다"
        print(line)
    if cleared:
        # **이 표는 변화를 말하지 원인을 말하지 않는다.** 요구사항 §6.3 —
        # *"자동 복구를 주장하려면 해당 키의 재시도·처리 이력도 확인"*.
        # 여기서 원인을 적으면 검사기가 미정인 것을 확정하는 셈이 된다.
        print(f"\n  ⓘ 해소 {cleared}건은 상태가 변했다는 **사실**이다."
              " 왜 변했는지는 이 표가 말하지 않는다 —")
        print("     늦게 들어온 등록일 수도, 다른 경로의 보상일 수도,"
              " 사람이 손댄 것일 수도 있다.")
        print("     이유를 보려면 그 키의 처리 이력(issuance_histories 의"
              " 상태 전이)을 따로 읽는다.")
    return 0


def report_paths(rep: Path):
    """반복 디렉터리의 대조 보고서들을 **오래된 것부터** 낸다.

    ⚠️ **파일 이름을 사전순으로 정렬하면 틀린다.** 같은 초에 두 번 돌면 뒤엣것에
    `-2` 가 붙는데, `-`(0x2D) 가 `.`(0x2E) 보다 작아서 `…+0000-2.json` 이
    `…+0000.json` 보다 **앞선다.** `-10` 은 `-2` 보다도 앞선다(문자열 비교).
    실제로 정렬해 보고 확인했다.

    그래서 이름을 (시각, 접미사 번호)로 **쪼개서** 정렬한다. 시각 부분은 UTC 고정
    너비라 문자열 비교가 맞고, 접미사는 숫자로 견준다.

    **파일 내용은 안 읽는다.** 예전에는 `generated_at` 을 읽어 정렬했는데, 못 읽은
    파일을 맨 앞으로 보내는 바람에 **최신 보고서가 잘리면 옛 보고서가 최신 행세를
    했다** — 요약이 그 옛 결과로 `OK` 를 찍는다. 순서는 이름만으로 정한다.
    """
    rows = []
    for path in rep.glob("reconcile-*.json"):
        stem = path.stem[len("reconcile-"):]
        # `<시각>` 또는 `<시각>-<번호>`. 번호가 없으면 첫 번째다.
        head, _, tail = stem.rpartition("-")
        if head and tail.isdigit():
            rows.append((head, int(tail), path))
        else:
            rows.append((stem, 1, path))
    return [r[2] for r in sorted(rows, key=lambda r: (r[0], r[1]))]


def report_shape_problem(report):
    """대조 보고서가 아닌 이유. 보고서면 `None`.

    스키마 이름과 **판정이 실린 두 칸의 타입**을 본다. 요약이 그 둘만 접기 때문에,
    그 둘이 없으면 "결함 0" 과 구분이 안 된다.
    """
    if not isinstance(report, dict):
        return f"{type(report).__name__} 이다"
    if report.get("schema") != SCHEMA:
        return f"schema 가 {report.get('schema')!r} 다"
    counts = report.get("counts")
    if not isinstance(counts, dict):
        return "counts 가 없거나 사전이 아니다"
    unjudged = report.get("unjudged")
    if not isinstance(unjudged, list):
        return "unjudged 가 없거나 목록이 아니다"
    # **그릇만 보면 모자란다.** 재 보니 셋 다 다른 방식으로 나빴다.
    #   · 값이 문자열이면 접다가 TypeError 로 요약 전체가 죽는다
    #   · unjudged 에 숫자가 섞이면 정렬에서 죽는다
    #   · **키가 모르는 이름이면 아무 일도 안 난다** — 그 결함이 조용히 사라진다.
    #     터지는 것보다 이쪽이 나쁘다. 그래서 키도 아는 유형인지 본다.
    for name, n in counts.items():
        if name not in ALL_TYPES:
            return f"counts 에 모르는 유형 {name!r} 이 있다"
        # bool 은 int 의 하위형이다. True 를 1건으로 세지 않는다.
        if isinstance(n, bool) or not isinstance(n, int) or n < 0:
            return f"counts[{name!r}] 이 음이 아닌 정수가 아니다"
    for name in unjudged:
        if name not in ALL_TYPES:
            return f"unjudged 에 모르는 유형 {name!r} 이 있다"
    return None


def latest_report(rep: Path):
    """가장 나중 대조 결과.

    셋을 가른다 — 보고서가 **없으면** `None`("대조 안 함"), 있는데 **못 읽으면**
    `{"unreadable": …}`, 정상이면 그 내용이다.

    ⚠️ **못 읽었다고 이전 보고서로 물러서지 않는다.** 최신 쓰기가 잘린 상황에서
    옛 결과를 최신으로 내면, 그 사이에 생긴 유실이 요약에서 사라진다.
    """
    paths = report_paths(rep)
    if not paths:
        return None
    path = paths[-1]
    try:
        report = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        return {"unreadable": f"{path.name} — {type(e).__name__}"}
    # **JSON 으로 읽혔다고 대조 보고서인 것은 아니다.** `{}` 도 유효한 JSON 이고,
    # 그것을 그대로 접으면 유형이 하나도 없으니 **깨끗한 대조로 보인다.** 이름만
    # 맞는 남의 파일도 마찬가지다. 모양을 확인하고, 아니면 판정 불가로 올린다.
    problem = report_shape_problem(report)
    if problem:
        return {"unreadable": f"{path.name} — {problem}"}
    return report


def unused_path(rep: Path, generated_at: str):
    """아직 없는 이름. 같은 초에 두 번 돌아도 앞 결과를 안 지운다."""
    stamp = generated_at.replace(":", "").replace("-", "")
    candidate = rep / f"reconcile-{stamp}.json"
    n = 2
    while candidate.exists():
        candidate = rep / f"reconcile-{stamp}-{n}.json"
        n += 1
    return candidate


# 심각도 순위. **종료코드 크기순이 아니다** — 결함(1)이 판정 불가(3)·보류(4)보다
# 나쁘다. 묶음에서 가장 나쁜 것을 고르려면 이 순위로 견줘야 한다.
SEVERITY = {0: 0, 4: 1, 3: 2, 1: 3}
CODE_NAME = {0: "정상", 1: "결함", 3: "판정 불가", 4: "보류"}


def worst_code(codes):
    """묶음의 종료코드. 하나라도 결함이면 결함이다."""
    return max(codes, key=lambda c: SEVERITY.get(c, 3)) if codes else 0


def run_one(rep, out_path=None):
    report = reconcile(rep)
    # 실행별로 남기고 **덮어쓰지 않는다.** 덮어쓰면 "다시 대조했더니 해소됐다" 를
    # 보여 줄 상대가 사라진다 — 그 비교가 늦은 등록과 진짜 유실을 가르는 유일한 수단이다.
    # 이름이 초 단위라 같은 초에 두 번 돌면 앞 결과가 사라지므로, 비어 있는 이름을
    # 찾을 때까지 뒤에 번호를 붙인다. 마이크로초를 안 쓰는 이유는 사람이 읽어서다.
    out = Path(out_path) if out_path else unused_path(rep, report["generated_at"])
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    code = print_report(report)
    print(f"  → {out}")
    return code


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("paths", nargs="+", metavar="경로")
    p.add_argument("--diff", action="store_true", help="대조 결과 두 개를 비교한다")
    p.add_argument("--out", help="결과 JSON 을 쓸 경로. 반복 하나일 때만 쓴다")
    args = p.parse_args()

    if args.diff:
        if len(args.paths) != 2:
            p.error("--diff 는 대조 결과 JSON 두 개를 받는다")
        a, b = (json.loads(Path(x).read_text()) for x in args.paths)
        return print_diff(diff(a, b))

    if args.out and len(args.paths) != 1:
        p.error("--out 은 반복 하나일 때만 쓴다 — 여럿이면 서로 덮어쓴다")
    if len(args.paths) == 1:
        return run_one(Path(args.paths[0]), args.out)

    codes = []
    for path in args.paths:
        print(f"\n\u2500\u2500 {path}")
        # **하나가 결함이어도 멈추지 않는다.** 첫 반복에서 멈추면 나머지를 못 본다.
        codes.append(run_one(Path(path)))
    order = sorted(set(codes), key=lambda c: SEVERITY.get(c, 3))
    print(f"\n묶음 {len(codes)}개 — "
          + " · ".join(f"{CODE_NAME.get(c, c)} {codes.count(c)}" for c in order))
    return worst_code(codes)


if __name__ == "__main__":
    sys.exit(main())
