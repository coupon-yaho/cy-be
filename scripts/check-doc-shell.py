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

# **펜스는 정규식 하나가 아니라 스캐너로 찾는다.** CommonMark 의 규칙이 정규식에 안 맞는다 —
# 여는 펜스는 백틱이든 물결표든 셋 이상이고, 닫는 펜스는 **같은 문자로 그만큼 이상**이면
# 되며 들여쓰기가 여는 쪽과 같을 필요도 없다. 그것을 한 패턴에 욱여넣었더니 두 가지가 났다:
# `~~~bash` 블록이 **양쪽 계산에서 모두 빠져** 하한 가드도 못 잡았고, 닫는 펜스가 더 긴
# 정상 마크다운이 "못 본 펜스" 로 신고돼 **정상 문서가 CI 를 빨갛게** 만들 뻔했다.
#
# 스캐너가 여는 펜스와 닫는 펜스를 함께 보므로, 못 본 형태는 "안 닫힌 블록" 으로 드러난다.
OPENING = re.compile(
    r"^(?P<indent>[ \t]*)(?P<marks>`{3,}|~{3,})"
    r"(?P<info>[^\n]*)$")

# 붙여 넣어 돌리는 블록의 언어 태그. info string 은 첫 낱말만 언어다(``` bash title=... ).
SHELL = re.compile(r"^(?:bash|sh|shell|zsh|console)\b")

# 값 자리를 꺾쇠로 싼 것. 리다이렉트·프로세스 치환·히어독은 앞에서 걸러 낸다.
PLACEHOLDER = re.compile(r"<[^>\s][^>]{0,40}>")

# 자리표시자로 오해하면 안 되는 셸 문법. `<<EOF` · `<(cmd)` · `<&3` · `<<<"$x"`.
REAL_SHELL = re.compile(r"<<|<\(|<&")


def fences(body: str) -> tuple[list[tuple[int, str]], list[int]]:
    """셸 블록들과, 여는 펜스는 있는데 못 닫힌 줄들.

    CommonMark 대로 본다 — 닫는 펜스는 <b>같은 문자로 여는 것만큼 이상</b>이면 되고,
    들여쓰기가 같을 필요는 없다. 그 둘을 강제하면 <b>정상 문서가 빨개진다.</b>
    """
    lines = body.splitlines()
    blocks: list[tuple[int, str]] = []
    unterminated: list[int] = []
    index = 0
    while index < len(lines):
        opened = OPENING.match(lines[index])
        if not opened:
            index += 1
            continue

        marks = opened.group("marks")
        closing = re.compile(r"^[ \t]*" + re.escape(marks[0])
                             + "{" + str(len(marks)) + r",}[^\S\n]*$")
        shell = bool(SHELL.match(opened.group("info").strip()))

        end = index + 1
        while end < len(lines) and not closing.match(lines[end]):
            end += 1
        if end >= len(lines):
            # **셸 블록일 때만 신고한다.** 이 저장소의 docs/PRD-v4.15.md 는 표 안에서
            # 백틱 여덟~열넷을 **장식 줄**로 쓴다 — 코드 펜스가 아닌데 규칙상 펜스로
            # 읽히고, 서로 열고 닫다가 마지막 하나가 안 닫힌 채로 남는다. 그것을
            # 신고하면 **정상 문서가 빨개진다**. 이 검사가 지키는 것은 셸 블록이다.
            if shell:
                unterminated.append(index + 1)
            break
        if shell:
            blocks.append((index + 2, "\n".join(lines[index + 1:end])))
        index = end + 1
    return blocks, unterminated


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

            found, unterminated = fences(body)
            for line in unterminated:
                missed.append((rel, line))

            for line, block in found:
                checked += 1
                result = parseable(block, work)
                if result.returncode:
                    broken.append((rel, line, clean(result.stderr, work, line)))

    for rel, line, detail in broken:
        print(f"::error file={rel},line={line}::셸 블록의 문법이 깨졌다 — {detail}")
    for rel, line in missed:
        print(f"::error file={rel},line={line}::셸 블록이 안 닫혔다 — 닫는 펜스를 못 찾았다. "
              f"이 검사가 못 보는 펜스 형태이거나 문서가 실제로 깨졌다")

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
