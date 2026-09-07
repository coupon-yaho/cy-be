#!/usr/bin/env python3
"""문서 안 셸 블록의 문법을 본다.

CI 의 «셸 스크립트 검사» 는 `git ls-files '*.sh'` 만 훑는다. 그래서 **붙여 넣어 돌리라고
둔 문서 블록은 아무도 안 본다** — docs/17 의 게이트 실행 절차는 함수 덩어리이고,
CY-939·CY-941 이 거기와 docs/14 에 `sh -c '...' _ "$q"` 같은 중첩 인용을 넣었다. 그 두
티켓에서는 사람이 손으로 `bash -n` 을 돌려 확인했다. 다음 사람은 안 한다.

**무엇을 못 잡는지 먼저 적는다.** `bash -n` 은 **파싱만** 한다. 돌연변이 셋을 실제로
태워 봤다(docs/17 블록 #4):

* 닫는 작은따옴표 하나 삭제 → **잡는다**
* `sh -c '...'` 를 `sh -c "..."` 로 치환 → **못 잡는다.** 문법은 맞고, 안쪽 `$1`·`$VAR` 가
  호스트에서 미리 펴져 컨테이너에서 빈 문자열이 된다 — CY-941 이 막은 바로 그 버그다
* 작은따옴표 안의 큰따옴표 하나 삭제 → **못 잡는다**

그래서 이 검사의 초록은 <b>"중첩 인용이 맞다" 가 아니라 "파싱은 된다"</b> 다. shellcheck 도
그 둘을 못 잡으므로 아래 판단은 그대로다.

**shellcheck 는 안 건다.** 블록은 조각이라 정의 안 된 변수·함수가 정상이고(SC2154 등),
그것을 만족시키려 고치면 절차가 아니라 린터용 코드가 된다. CI 가 `docs/measurements/` 를
린터에서 빼 두는 것과 같은 판단이다.

**블록마다 따로 본다.** 블록 하나가 <b>사람이 한 번에 붙여 넣는 단위</b>라 각자 완결이어야
한다. 이어 붙이면 두 블록의 따옴표 불균형이 서로 상쇄돼 **누락**이 생긴다. 실측에서는
따로 봐도 이어 붙여도 전부 통과하므로 오늘 오탐 차이는 없다 — 더 엄격한 쪽을 고른 것이다.
(한때 여기 <i>"이어 붙이면 앞 블록의 `cd` 나 변수가 뒤 블록의 전제처럼 보인다"</i> 고
적었는데 **틀렸다.** `bash -n` 은 실행을 안 하므로 `cd` 도 변수도 평가하지 않는다.)

**{@code docs/} 만 본다.** 저장소 밖 `.md`(루트 `README.md`·`perf/README.md` 등)에도 셸
블록이 있지만, 이 검사가 생긴 이유가 <b>붙여 넣어 돌리는 운영 절차</b>라 그것이 사는
자리부터 건다. 넓히는 것은 그 문서들의 블록이 실제로 절차인지 본 뒤다.

**자리표시자는 그 토큰만 바꾼다.** `<ID>`·`<시드의 as_of>` 처럼 사람이 값을 채우라고 둔 것은
그대로는 셸이 아니라, 안전한 낱말로 치환하고 나머지는 그대로 본다.

두 번 좁혔다. ① 한때 그런 것이 하나라도 들면 **블록 전체**를 건너뛰었는데, 이 저장소 문서는
셸 블록 주석에 한글 설명을 길게 쓰는 문체라 — `docs/14` 의 한 블록은 스물여섯 줄 중 열다섯이
주석이다 — 주석 한 줄의 `<값>` 때문에 블록이 통째로 사라졌다. ② 그다음엔 **그 줄**을 지웠는데,
그러면 그 줄의 따옴표와 문법도 같이 사라진다 — `docs/measurement-protocol.md` 의
`echo "... <측정 실패>로 기록할 것 ..." >&2` 는 **멀쩡한 실행 줄**이고, 그 줄의 닫는 따옴표가
깨져도 초록이었다.
"""
import pathlib
import re
import subprocess
import sys
import tempfile

# **들여쓴 펜스도 잡는다.** `^```` 로 0열만 보면 리스트 항목 안의 블록이 통째로 빠진다 —
# 실측에서 docs/14 의 다섯이 그랬고, 그중 하나가 이 검사가 막겠다고 한 그 블록이었다.
# 백틱은 **셋 이상**(`{3,}`)이라 표준 세 개와 네 개 이상을 다 받고, 언어 태그도
# shell·zsh·console 까지 받는다. `[^\S\n]` 는 개행 아닌 공백이다.
FENCE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<ticks>`{3,})(?:bash|sh|shell|zsh|console)"
    r"[^\n]*\n(?P<body>.*?)^(?P=indent)(?P=ticks)[^\S\n]*$",
    re.S | re.M)

# 여는 펜스만 센다. FENCE 가 잡은 수와 어긋나면 **못 본 블록이 있다는 뜻**이다.
OPENING = re.compile(r"^[ \t]*`{3,}(?:bash|sh|shell|zsh|console)\b", re.M)

# 값 자리를 꺾쇠로 싼 것. 리다이렉트·프로세스 치환·히어독은 앞에서 걸러 낸다.
PLACEHOLDER = re.compile(r"<[^>\s][^>]{0,40}>")

# 자리표시자로 오해하면 안 되는 셸 문법. `<<EOF` · `<(cmd)` · `<&3` · `<<<"$x"`.
REAL_SHELL = re.compile(r"<<|<\(|<&")


def redact(line: str) -> str:
    """자리표시자 <b>토큰만</b> 안전한 낱말로 바꾼다.

    ⚠️ **줄째 지우면 안 된다.** 그 줄의 따옴표와 셸 문법이 함께 사라져,
    {@code docs/measurement-protocol.md} 의 실제 실행 줄처럼 <b>자리표시자를 낱말로 품은
    멀쩡한 명령</b>이 검사에서 빠진다 — 그 줄의 닫는 따옴표가 깨져도 초록이다.
    토큰만 바꾸면 따옴표와 구조가 남는다.

    셸 문법이 섞인 줄은 손대지 않는다. {@code <<EOF}·{@code <(cmd)}·{@code <&3} 은
    자리표시자가 아니라 진짜 문법이고, 바꾸면 그것이 오히려 문법을 깬다.
    """
    if REAL_SHELL.search(line):
        return line
    return PLACEHOLDER.sub("PLACEHOLDER", line)


def parseable(block: str, work: pathlib.Path) -> subprocess.CompletedProcess:
    redacted = "\n".join(redact(line) for line in block.splitlines())
    work.write_text(redacted, encoding="utf-8")
    return subprocess.run(["bash", "-n", str(work)], capture_output=True, text=True)


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    docs = sorted(root.glob("docs/**/*.md"))
    if not docs:
        print("::error::docs 에서 .md 를 못 찾았다 — 스캔이 깨졌다")
        return 1

    checked = 0
    broken = []
    missed = []
    with tempfile.TemporaryDirectory() as tmp:
        work = pathlib.Path(tmp) / "block.sh"
        for doc in docs:
            body = doc.read_text(encoding="utf-8")
            rel = doc.relative_to(root)

            found = list(FENCE.finditer(body))
            # **하한 가드.** 0건만 막으면 정규식이 조용히 좁아져도 초록이다 — 이 저장소가
            # CY-913 에서 겪은 그대로다(그 교훈이 build.yml 에 적혀 있다). 여는 펜스 수와
            # 맞대면 새 펜스 형태가 들어와도 "조용히 빠짐" 이 아니라 "빨감" 이 된다.
            opening = len(OPENING.findall(body))
            if opening != len(found):
                missed.append((rel, opening, len(found)))

            for match in found:
                checked += 1
                line = body[:match.start("body")].count("\n") + 1
                result = parseable(match.group("body"), work)
                if result.returncode:
                    broken.append((rel, line, clean(result.stderr, work, line)))

    for rel, line, detail in broken:
        print(f"::error file={rel},line={line}::셸 블록의 문법이 깨졌다 — {detail}")
    for rel, opening, seen in missed:
        print(f"::error file={rel}::셸 펜스가 {opening}개인데 {seen}개만 잡혔다 — "
              f"이 검사가 못 보는 펜스 형태가 들어왔다")

    if not checked:
        print("::error::검사할 셸 블록을 하나도 못 찾았다")
        return 1

    print(f"셸 블록 {checked}개 검사")
    return 1 if broken or missed else 0


def clean(stderr: str, work: pathlib.Path, offset: int) -> str:
    """진단을 사람이 쓸 수 있는 문장으로 만든다.

    ⚠️ **마지막 줄을 쓰면 안 된다.** {@code bash -n} 은 보통 두 줄을 내는데 첫 줄이
    진단이고 둘째 줄이 문제의 소스다 — 마지막만 남기면 <b>무엇이 잘못됐는지가 사라진다.</b>
    임시 경로도 지운다. CI 로그에 러너의 tmp 경로가 찍혀 봐야 아무 뜻이 없다.
    """
    lines = [one for one in stderr.strip().splitlines() if one]
    if not lines:
        return "bash -n 이 아무 말도 안 했다"
    # bash 가 주는 줄 번호는 블록 안 기준이라 문서 기준으로 옮긴다.
    return re.sub(r"^" + re.escape(str(work)) + r": line (\d+): ",
                  lambda m: f"문서 {offset + int(m.group(1)) - 1}행: ",
                  lines[0])


if __name__ == "__main__":
    sys.exit(main())
