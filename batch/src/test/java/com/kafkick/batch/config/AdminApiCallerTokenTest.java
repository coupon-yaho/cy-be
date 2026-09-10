// 문서·스크립트·주석이 관리자 API 를 부를 때 토큰 헤더를 싣는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * <b>관문이 켜진 배포에서 절차가 401 로 죽는 것을 막는다.</b>
 *
 * <p>{@code BATCH_ADMIN_AUTH_REQUIRED} 는 <b>계층마다 다르다</b> — 앱 기본 {@code true},
 * 업무 포트를 안 여는 {@code batch.yml} 은 {@code false}, 포트를 여는
 * {@code batch-expose.yml} 은 다시 {@code true} 고, {@code batch-verify.yml} 은
 * <b>손잡이도 없이 {@code "true"} 로 박혀 있다</b>(CI 가 환경변수로 못 끄는지까지 본다).
 * 그래서 절차를 <b>포트가 닫힌 기본값에서만 써 보면</b> 헤더 없이도 다 통과하고, 정작
 * 포트를 여는 날 — 관제를 붙이거나 제출물을 뽑는 날 — 첫 호출부터 막힌다.
 *
 * <p>문서·스크립트·주석에서 관리자 API 를 부르는 자리 <b>열넷 중 아홉</b>이 그 상태였다
 * (CY-939). {@code docs/14} 는 <b>오버레이를 얹으라고 명시해 놓고</b> 헤더가 없어,
 * 적힌 대로면 <b>기동조차 안 됐다</b> — {@code AdminTokenConfig} 가 토큰 없이 관문을
 * 켜는 것을 거절한다.
 *
 * <p>지금은 <b>열일곱</b>이다 — CY-954 가 {@code /reports/residual/targets} 예시를 하나 더했다.
 *
 * <h2>왜 기계가 봐야 하나</h2>
 *
 * <p><b>사람이 훑는 것으로 안 된다는 근거가 이력에 있다.</b> 관문은 CY-742 가
 * 2026-08-29 07:58 에 넣었고, <b>여덟 시간 뒤</b> CY-744 가 <i>"9091 로 쓰는 자리를 전부
 * 맞춘다"</i> 며 커밋 메시지에 그 자리들을 <b>이름으로 열거하고</b> 줄을 다시 썼다 —
 * 그러고도 헤더는 안 붙였다. 세 파일을 한 커밋에서 훑은 사람이 못 본 것이다.
 *
 * <p><b>헤더는 관문이 꺼져 있어도 무해하다.</b> 값이 비면 curl 이 그 헤더를 <b>아예 안
 * 보낸다</b>(빈 값 헤더는 삭제 지시다) — 필터가 등록 안 된다는 것보다 강한 사실이고,
 * 요청이 바이트 단위로 이전과 같다는 뜻이다. 그래서 "관문이 켜질 때만 붙인다" 로 가르지
 * 않고 <b>늘 싣는다</b> — 가르면 그 판단이 다시 사람 몫이 되고, 그것이 이 결함의 원인이다.
 *
 * <h2>범위</h2>
 *
 * <p><b>알림 규칙 파일은 안 본다.</b> 거기 적힌 경로는 <b>산문</b>이지 붙여 넣을 명령이
 * 아니고, 그 자리마다 헤더를 끼우면 이미 긴 문구에서 <b>운영자가 읽어야 할 판단</b>이
 * 묻힌다. 대신 그 description 이 {@code docs/13 §4} 를 가리키고 그 절차 위에 관문 경고가
 * 있다. <b>정한 경계</b>이지 빠뜨린 것이 아니다.
 *
 * <p>반대로 <b>자바 주석은 본다.</b> {@code VerifyReportController} 의 {@code <pre>} 안에
 * 붙여 넣으라고 둔 {@code curl} 이 있는데, <b>같은 파일 안에서 한쪽만 헤더가 있었다</b> —
 * 산문과 명령이 갈리는 자리가 아니라 그냥 놓친 자리다.
 *
 * <p><b>스캔 범위는 {@code batch/build.gradle} 에 똑같이 선언한다</b> —
 * {@code externalContractFiles} 가 {@code docs/*.md}·{@code scripts/*.sh} 를,
 * {@code scannedJavaSources} 가 자바를 이미 건다. 안 걸면 문서만 고친 라운드에서
 * {@code :batch:test} 가 UP-TO-DATE 로 건너뛴다 — 검사가 있는데 안 도는 상태다.
 *
 * <p>⚠️ <b>자바는 {@code src} 아래만 본다.</b> {@code scannedJavaSources} 가 거는 범위가
 * 정확히 그것이라({@code **}{@code /src/**}{@code /*.java}) <b>맞춰서 자른 것</b>이다.
 * 한때 {@code /build/} 만 빼고 전부 훑으면서 <i>"오늘은 1459 대 1459 로 같다"</i> 고
 * 적어 뒀는데, 그것은 <b>우연이지 계약이 아니었다</b> — {@code src} 밖에 {@code .java} 가
 * 하나 생기는 순간 그 파일은 스캔에는 잡히고 캐시 키에는 없어, 그것만 바꾼 라운드에서
 * 검사가 안 돈다.
 *
 * <p>⚠️ <b>{@code docs} 는 최상위만 본다({@code Files.list}).</b> Gradle 쪽 글롭
 * {@code include '*.md'} 가 {@code /} 를 안 넘는 것과 <b>맞춰 둔 것</b>이다 — 한쪽만
 * 재귀로 "고치면" 범위와 캐시 키가 조용히 갈린다. 형제들이 {@code Files.walk} 를 쓰는
 * 것과 다른 이유가 이것이다.
 */
class AdminApiCallerTokenTest {

    private static final Path REPO_ROOT = Path.of("..");

    private static final String ADMIN_PATH = "/api/v1/admin";

    /** {@code AdminTokenFilter#HEADER} 와 같아야 한다. 아래 시험이 그것을 본다. */
    private static final String HEADER = "X-Batch-Admin-Token";

    /** 붙여 넣으면 요청이 나가는 것들. 컨테이너 안에서 {@code wget} 을 쓰는 자리도 있다. */
    private static final List<String> CLIENTS = List.of("curl", "wget");

    /**
     * <b>헤더가 든 값의 형태.</b> 리터럴을 박아 두면 그것이 그대로 나가고, 관문이 켜지면
     * 이름은 맞는데 값이 틀려 <b>401 은 그대로다</b>. 실제로 {@code VerifyReportController}
     * 의 붙여 넣기 예시 둘이 {@code "X-Batch-Admin-Token: …"} 이었다.
     *
     * <p><b>{@code 헤더:} 형태일 때만 본다.</b> 이름만 언급하는 산문이 있다 —
     * {@code docs/15} 의 보안 표가 <i>"CY-742 가 공유 비밀 헤더(`X-Batch-Admin-Token`)를
     * 얹었다"</i> 로 <b>백틱 안에 이름만</b> 쓴다. 거기까지 값을 요구하면 그 문장에
     * {@code $BATCH_ADMIN_TOKEN} 을 끼워 넣는 <b>틀린 고침</b>을 부른다.
     */
    private static final Pattern HEADER_VALUE =
            Pattern.compile(java.util.regex.Pattern.quote(HEADER) + ":\\s*(\\S+)");

    /**
     * <b>정확한 수를 못 박는다.</b> {@code isNotEmpty()} 만 두면 스캔이 조용히 좁아져도
     * (경로 상수 오타, 클라이언트 이름 변경, 줄이 하나 밀려 창을 벗어남) 위반 목록이
     * 비고 <b>영원히 초록</b>이다 — 이 클래스가 막으려는 실패 모드가 정확히 그 누락이다.
     * 형제 {@code StuckBeforeBindingIsCentralizedTest} 도 수를 못 박는다.
     *
     * <p>호출을 <b>늘리는</b> 티켓이 이 수를 같이 올려야 한다. 그것이 이 상수의 값이다.
     */
    private static final int KNOWN_CALLS = 17;

    /** 이 파일. {@link #scanned()} 가 자기 표본을 안 잡게 뺀다. */
    private static final String SELF = "AdminApiCallerTokenTest.java";

    private record Call(Path file, int line, String context) {
    }

    @Test
    @DisplayName("관리자 API 호출은 전부 토큰 헤더를 싣는다")
    void everyAdminApiCallCarriesTheToken() throws IOException {
        List<String> offenders = adminApiCalls().stream()
                .filter(call -> !call.context().contains(HEADER))
                .map(call -> REPO_ROOT.relativize(call.file()) + ":" + call.line())
                .toList();

        assertThat(offenders)
                .as("관문이 켜진 배포(batch-expose.yml · batch-verify.yml)에서 이 호출들은 "
                        + "401 이다. 값이 비면 curl 이 헤더를 아예 안 보내므로 관문이 꺼진 "
                        + "구성에서도 요청은 그대로다 — 늘 실어 두면 된다")
                .isEmpty();
    }

    /**
     * <b>이름이 맞아도 값이 리터럴이면 401 은 그대로다.</b> 헤더를 실었다는 사실만 보면
     * {@code "X-Batch-Admin-Token: …"} 같은 자리표시자가 통과하고, 그것을 붙여 넣은
     * 사람은 <b>헤더를 실었는데 왜 401 인지</b>를 디버깅한다.
     */
    @Test
    @DisplayName("헤더 값은 환경변수로 편다 — 리터럴 자리표시자가 아니다")
    void tokenValueComesFromTheEnvironment() throws IOException {
        List<String> literals = new ArrayList<>();
        for (Call call : adminApiCalls()) {
            Matcher value = HEADER_VALUE.matcher(call.context());
            while (value.find()) {
                if (!value.group(1).contains("$")) {
                    literals.add(REPO_ROOT.relativize(call.file()) + ":" + call.line()
                            + " → " + value.group(1));
                }
            }
        }

        assertThat(literals)
                .as("붙여 넣으면 그 값이 그대로 나간다. 관문이 켜지면 이름은 맞고 값이 "
                        + "틀려 401 이고, 헤더를 실었으니 맞겠거니 하고 다른 데를 판다")
                .isEmpty();
    }

    @Test
    @DisplayName("스캔이 아는 만큼의 호출을 찾는다")
    void findsEveryKnownCall() throws IOException {
        List<Call> calls = adminApiCalls();

        assertThat(calls)
                .as("호출을 %d건 알고 있는데 %d건 잡혔다. 늘었으면 KNOWN_CALLS 를 올리고, "
                        + "줄었는데 지운 적이 없으면 스캔이 좁아진 것이다 — 그때는 위반이 "
                        + "안 나는 것이 아니라 안 보는 것이다", KNOWN_CALLS, calls.size())
                .hasSize(KNOWN_CALLS);
    }

    @Test
    @DisplayName("헤더 이름이 필터의 것과 같다")
    void headerMatchesTheFilter() throws Exception {
        // **리플렉션으로 읽는다.** 두 상수를 그냥 == 로 비교하면 javac 가 양쪽을 같은
        // 리터럴로 인라인해서 **런타임에 아무것도 안 재는 시험**이 된다(javap 로 확인:
        // ldc #9 두 번). 필드에서 읽으면 실제로 로드된 클래스의 값을 본다.
        Object actual = AdminTokenFilter.class.getField("HEADER").get(null);

        assertThat(actual)
                .as("필터가 보는 헤더와 이 검사가 찾는 헤더가 갈리면, 검사는 초록인데 "
                        + "절차는 401 이다")
                .isEqualTo(HEADER);
    }

    /**
     * {@code curl}·{@code wget} 으로 관리자 경로를 부르는 자리들. 클라이언트가 <b>같은
     * 줄이나 앞 세 줄</b>에 있는 것만 센다 — 산문에서 경로를 언급하는 것은 호출이 아니다.
     */
    private static List<Call> adminApiCalls() throws IOException {
        List<Call> calls = new ArrayList<>();
        for (Path file : scanned()) {
            List<String> lines = Files.readAllLines(file);
            for (int i = 0; i < lines.size(); i++) {
                if (!lines.get(i).contains(ADMIN_PATH)) {
                    continue;
                }
                String before = command(lines, i);
                if (CLIENTS.stream().noneMatch(before::contains)) {
                    continue;
                }
                calls.add(new Call(file, i + 1, before));
            }
        }
        return calls;
    }

    /**
     * <b>한 명령의 범위.</b> 경로줄에서 <b>줄이음({@code \\})을 거슬러</b> 올라가고,
     * 이어지지 않는 줄을 만나면 멈춘다.
     *
     * <p><b>줄 수로 세면 양쪽으로 가려진다.</b> 앞뒤 몇 줄을 보는 창은 <b>이웃 명령의
     * 헤더를 이 명령 것으로 인정</b>한다 — 헤더 있는 호출 위든 아래든, 헤더 없는 호출을
     * 몇 줄 옆에 붙이는 것만으로 통과한다. {@code docs/17} 은 거의 같은 블록 넷이 여덟 줄
     * 간격이라 복붙 한 번이면 나오는 모양이다.
     *
     * <p>줄이음은 <b>셸이 실제로 한 명령으로 읽는 경계</b>라 그 오해가 구조적으로 없다.
     * 저장소의 열넷이 전부 이 경계 안에 헤더를 갖는다(실측) — 창을 좁혀서 놓치는 것이 없다.
     */
    private static String command(List<String> lines, int pathLine) {
        int from = pathLine;
        while (from > 0 && lines.get(from - 1).stripTrailing().endsWith("\\")) {
            from--;
        }
        return String.join("\n", lines.subList(from, pathLine + 1));
    }

    /** 경로 구분자를 편다. 형제들과 같은 관용이다. */
    private static String slashed(Path path) {
        return path.toString().replace('\\', '/');
    }

    private static List<Path> scanned() throws IOException {
        try (Stream<Path> docs = Files.list(REPO_ROOT.resolve("docs"));
                Stream<Path> scripts = Files.list(REPO_ROOT.resolve("scripts"));
                Stream<Path> java = Files.walk(REPO_ROOT)) {
            return Stream.of(
                            docs.filter(path -> path.toString().endsWith(".md")),
                            scripts.filter(path -> path.toString().endsWith(".sh")),
                            java.filter(path -> path.toString().endsWith(".java"))
                                    // **입력 선언과 같은 범위로 자른다.**
                                    // scannedJavaSources 가 거는 것이 정확히 이 둘이다 —
                                    // src 아래, build 밖. 스캔이 그보다 넓으면 그 파일은
                                    // **잡히기는 하는데 캐시 키에는 없는** 상태가 되어,
                                    // 그것만 바꾼 라운드에서 UP-TO-DATE 로 안 돈다.
                                    //
                                    // 구분자를 먼저 편다 — 형제 넷이 같은 자리에서 같은
                                    // 모양을 쓴다(NoOrphanJavadocTest 등). 안 펴면 역슬래시
                                    // 플랫폼에서 두 filter 가 **둘 다 빗나가** 자바가 통째로
                                    // 안 잡히고, 그러면 위반 0 건이 "없다" 로 읽힌다.
                                    .filter(path -> slashed(path).contains("/src/"))
                                    .filter(path -> !slashed(path).contains("/build/"))
                                    // **자기 자신은 뺀다.** 아래 Detection 의 표본이
                                    // 일부러 헤더 없는 호출이라, 안 빼면 이 시험이
                                    // 자기 표본을 위반으로 신고하고 영영 빨갛다.
                                    .filter(path -> !path.getFileName().toString()
                                            .equals(SELF)))
                    .flatMap(stream -> stream)
                    .sorted()
                    .toList();
        }
    }

    /**
     * <b>스캔이 잡아야 할 것과 안 잡아야 할 것을 못 박는다.</b> 형제
     * {@code StuckBeforeBindingIsCentralizedTest.Detection} 과 같은 자리다 — 스캔이
     * 조용히 좁아지거나 넓어지는 것을 저장소 파일만으로는 못 잰다.
     */
    @Nested
    class Detection {

        @Test
        @DisplayName("클라이언트가 앞줄에 있어도 호출로 센다")
        void countsMultiLineInvocation() {
            assertThat(hits(List.of("curl -sS \\", "     http://x/api/v1/admin/verify"))).isOne();
            assertThat(hits(List.of("wget -qO- \\", "     http://x/api/v1/admin/verify"))).isOne();
        }

        @Test
        @DisplayName("산문의 경로 언급은 호출이 아니다")
        void ignoresProse() {
            assertThat(hits(List.of("`/api/v1/admin/verify` 는 202 를 낸다"))).isZero();
        }

        @Test
        @DisplayName("줄바꿈으로 이어 쓴 헤더를 위반으로 안 읽는다")
        void seesHeaderOnAPrecedingLine() {
            assertThat(violations(List.of(
                    "curl -sS \\",
                    "     -H \"X-Batch-Admin-Token: $BATCH_ADMIN_TOKEN\" \\",
                    "     http://x/api/v1/admin/verify"))).isZero();
        }

        @Test
        @DisplayName("헤더가 아예 없으면 위반이다")
        void flagsAMissingHeader() {
            assertThat(violations(List.of("curl -sS http://x/api/v1/admin/verify"))).isOne();
        }

        /**
         * <b>뒤를 안 보는 이유가 이것이다.</b> 창이 뒤로도 열려 있으면, 헤더 없는 호출이
         * 헤더 있는 호출 <b>위에 붙는 것만으로</b> 가려진다 — {@code docs/17} 처럼 같은
         * 블록이 몇 줄 간격으로 반복되는 문서에서 복붙 한 번이면 나오는 모양이다.
         */
        @Test
        @DisplayName("아래 호출의 헤더가 위 호출의 누락을 가리지 않는다")
        void doesNotLetALaterHeaderMaskAnEarlierMiss() {
            assertThat(violations(List.of(
                    "curl -sS http://x/api/v1/admin/verify/runs/1",
                    "",
                    "curl -sS \\",
                    "     -H \"X-Batch-Admin-Token: $BATCH_ADMIN_TOKEN\" \\",
                    "     http://x/api/v1/admin/verify/runs/2"))).isOne();
        }

        /**
         * <b>위 호출의 헤더가 아래 호출의 누락을 가리지 않는다.</b> 앞뒤 몇 줄을 보는
         * 창에서는 이쪽이 뚫린다 — 헤더 있는 호출 <b>바로 다음 줄</b>에 헤더 없는 호출을
         * 붙이면 통과한다. 줄이음 경계는 그 둘을 다른 명령으로 읽는다.
         */
        @Test
        @DisplayName("위 호출의 헤더가 아래 호출의 누락을 가리지 않는다")
        void doesNotLetAnEarlierHeaderMaskALaterMiss() {
            assertThat(violations(List.of(
                    "curl -sS -H \"X-Batch-Admin-Token: $BATCH_ADMIN_TOKEN\" \\",
                    "     http://x/api/v1/admin/verify/runs/1",
                    "curl -sS http://x/api/v1/admin/verify/runs/2"))).isOne();
        }

        @Test
        @DisplayName("이어진 줄은 한 명령으로 읽는다")
        void keepsAContinuedCommandTogether() {
            assertThat(violations(List.of(
                    "curl -sS \\",
                    "     -H \"X-Batch-Admin-Token: $BATCH_ADMIN_TOKEN\" \\",
                    "     http://x/api/v1/admin/verify"))).isZero();
        }

        private long hits(List<String> lines) {
            return windows(lines).size();
        }

        private long violations(List<String> lines) {
            return windows(lines).stream().filter(text -> !text.contains(HEADER)).count();
        }

        private List<String> windows(List<String> lines) {
            List<String> found = new ArrayList<>();
            for (int i = 0; i < lines.size(); i++) {
                String before = command(lines, i);
                if (lines.get(i).contains(ADMIN_PATH)
                        && CLIENTS.stream().anyMatch(before::contains)) {
                    found.add(before);
                }
            }
            return found;
        }
    }
}
