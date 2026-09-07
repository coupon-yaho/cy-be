// 문서·스크립트가 DB 비밀번호를 호스트 셸로 꺼내지 않는지 확인합니다.
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
 * <b>{@code MYSQL_PWD} 를 호스트 쪽에서 만들지 않는다.</b> 값을 컨테이너 <b>안</b>에서
 * {@code MYSQL_ROOT_PASSWORD} 로부터 옮긴다.
 *
 * <pre>
 * docker compose -f base.yml exec -T mysql sh -c \
 *   'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot "$1"' _ "$SCHEMA"
 * </pre>
 *
 * <p>이유는 둘이고, <b>둘 다 저장소가 이미 값을 치렀다.</b>
 *
 * <p>① <b>리터럴은 기본값이 아닌 배포에서 그냥 깨진다.</b> {@code base.yml} 이
 * {@code MYSQL_ROOT_PASSWORD: ${DB_ROOT_PASSWORD:-root}} 라 <b>기본값에서는 돌고</b>,
 * {@code DB_ROOT_PASSWORD} 를 주는 순간 전부 실패한다. {@code docs/17} 의 게이트 실행
 * 절차 <b>다섯 자리</b>가 그 상태였다(CY-941) — {@code AdminApiCallerTokenTest} 가 잡은
 * 것과 <b>같은 모양</b>이다: 기본 구성에서만 써 본 절차.
 *
 * <p>② <b>호스트에서 펴면 값이 {@code ps} 에 남는다.</b>
 * {@code -e MYSQL_PWD="${DB_ROOT_PASSWORD:-root}"} 는 그 값을 호스트 docker 프로세스의
 * 명령행에 싣는다. 그 근거를 {@code scripts/pour-batch-meta.sh} 가 적어 두고 옳은 형태를
 * 쓰고 있었는데, 문서 둘이 바로 그것을 하고 있었다.
 *
 * <p><b>스캔 범위는 {@code batch/build.gradle} 의 {@code externalContractFiles} 에
 * 똑같이 선언한다</b> — {@code docs/*.md}·{@code scripts/*.sh}(CY-939)에 더해
 * <b>{@code infra} 아래 {@code .sh} 를 재귀로</b> 건다. 열셋 중 <b>다섯이 그 infra
 * 파일</b>이라 안 걸면 그것만 바뀐 라운드에서 검사가 안 돈다.
 *
 * <p>⚠️ <b>{@code Files.walk} 는 심볼릭 링크를 안 따라가고 Gradle 의 {@code fileTree} 는
 * 따라간다.</b> {@code infra} 아래에 링크된 디렉터리가 생기면 <b>입력에는 있고 스캔에는
 * 없는</b> 파일이 생긴다 — 지금 그런 링크는 없다. 생기는 날 둘 중 하나를 맞춘다.
 *
 * <p><b>{@code -p}·{@code --password} 도 함께 막는다</b>({@link #NAKED_PASSWORD}).
 * 스캔 범위 안에 그 형태가 <b>한 건도 없어서</b>(실측) 걸어도 오탐이 없다.
 * {@code docs/measurements/} 의 옛 실측 스크립트가 {@code mysql -proot} 를 스물다섯 번
 * 쓰지만 그것은 <b>범위 밖</b>이다: {@code Files.list} 라 {@code docs} 최상위만 보고,
 * CI 도 그 디렉터리를 린터에서 일부러 뺀다(<i>"과거 실측을 재현하는 기록이라 린터를
 * 만족시키려고 고치면 그 파일이 더 이상 그 숫자를 낸 스크립트가 아니다"</i>).
 */
class DbPasswordHandlingTest {

    private static final Path REPO_ROOT = Path.of("..");

    /**
     * <b>{@code MYSQL_PWD} 에 무엇을 대입하는가.</b> 값이 {@code $MYSQL_ROOT_PASSWORD} 면
     * 컨테이너 안에서 편 것이고, 그 밖이면 호스트가 만든 것이다.
     */
    private static final Pattern ASSIGNMENT = Pattern.compile("MYSQL_PWD=(\\S+)");

    /**
     * 컨테이너 안에서 펴는 허용 형태. 따옴표 유무 둘 다 받고, root 가 아닌 계정도 받는다
     * ({@code perf/lib/common.sh} 가 {@code MYSQL_PASSWORD} 로 앱 계정을 쓴다 — 지금은
     * 범위 밖이지만 넓히는 날 정상 코드가 위반으로 잡히면 안 된다).
     */
    private static final Pattern FROM_CONTAINER =
            Pattern.compile("^\"?\\$\\{?MYSQL_(?:ROOT_)?PASSWORD\\}?\"?$");

    /**
     * <b>{@code -e MYSQL_PWD=} 는 값과 무관하게 위반이다.</b> 그 자리는 <b>호스트 명령행</b>
     * 이라, 무엇을 넣든 값이 거기 실린다.
     *
     * <p>값만 보던 앞 규칙은 <b>가장 일어나기 쉬운 회귀를 통과시켰다</b> —
     * {@code sh -c} 래핑을 군더더기로 보고 걷어내면서 변수 이름만 남기면
     * ({@code -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD"}) 텍스트는 그대로라 통과한다. 그런데
     * <b>그 이름은 컨테이너 안 이름</b>이라 호스트에서는 빈 값이 되어
     * {@code ERROR 1045 (using password: NO)} 로 죽고, 셸에 export 해 뒀다면 값이
     * {@code ps} 로 샌다 — 이 검사가 막으려던 둘이 그대로다.
     *
     * <p><b>자리표시자는 뺀다</b>({@code MYSQL_PWD=<값>}). 이 규칙을 <b>설명하는 산문</b>이
     * 그 형태로 금지 예시를 든다 — {@code scripts/pour-batch-meta.sh} 의 근거 주석과
     * {@code docs/14} 가 그렇다. 안 빼면 <b>검사가 자기 근거를 못 적게 만든다.</b>
     * 금지 형태를 문서에 쓸 때는 값을 꺾쇠로 싸면 된다.
     */
    private static final Pattern ON_HOST_COMMAND_LINE =
            Pattern.compile("(?:-e|--env)\\s+\"?MYSQL_PWD=(?!<)");

    /**
     * {@code -p}·{@code --password} 로 넘기는 형태. {@code MYSQL_PWD} 를 쓰는 이유가 바로
     * 이것을 피하려는 것이라, 그 규칙을 산문에만 적어 두면 아무도 안 지킨다.
     */
    private static final Pattern NAKED_PASSWORD =
            Pattern.compile("mysql\\b[^\\n]*?(?:--password[= ]|\\s-p[A-Za-z0-9\"$])");

    /**
     * <b>정확한 수를 못 박는다.</b> {@code isEmpty()} 만 두면 스캔이 조용히 좁아져도
     * (경로가 바뀌거나 정규식이 어긋나) 위반이 안 나고 <b>영원히 초록</b>이다 —
     * 대입을 늘리는 티켓이 이 수를 같이 올린다.
     */
    private static final int KNOWN_ASSIGNMENTS = 13;

    private record Assignment(Path file, int line, String value) {
    }

    @Test
    @DisplayName("MYSQL_PWD 는 컨테이너 안에서 MYSQL_ROOT_PASSWORD 로부터 편다")
    void neverBuildsThePasswordOnTheHost() throws IOException {
        List<String> offenders = assignments().stream()
                .filter(one -> !FROM_CONTAINER.matcher(one.value()).matches())
                .map(one -> where(one) + " → MYSQL_PWD=" + one.value())
                .toList();

        assertThat(offenders)
                .as("리터럴이면 DB_ROOT_PASSWORD 를 주는 배포에서 그냥 실패하고, 호스트에서 "
                        + "편 값이면 ps 에 남는다. 컨테이너가 이미 MYSQL_ROOT_PASSWORD 를 "
                        + "갖고 있으므로(base.yml) 거기서 옮기면 둘 다 없다")
                .isEmpty();
    }

    /** {@link #ON_HOST_COMMAND_LINE} 에 근거를 적었다. */
    @Test
    @DisplayName("MYSQL_PWD 를 -e 로 넘기지 않는다 — 값과 무관하게 호스트 명령행이다")
    void neverPassesThePasswordThroughDockerEnvFlag() throws IOException {
        assertThat(matching(ON_HOST_COMMAND_LINE))
                .as("그 자리는 호스트 명령행이라 무엇을 넣든 값이 거기 실린다. "
                        + "sh -c 안에서 MYSQL_ROOT_PASSWORD 를 옮기면 호스트에 아무것도 "
                        + "안 꺼낸다")
                .isEmpty();
    }

    /** {@link #NAKED_PASSWORD} 에 근거를 적었다. */
    @Test
    @DisplayName("비밀번호를 -p·--password 인자로 주지 않는다")
    void neverPassesThePasswordAsAnArgument() throws IOException {
        assertThat(matching(NAKED_PASSWORD))
                .as("인자로 주면 argv 에 실린다. MYSQL_PWD 를 쓰는 이유가 그것이다 — "
                        + "docs/14 가 그 규칙을 적어 두고 있었는데 아무도 재지 않았다")
                .isEmpty();
    }

    private static List<String> matching(Pattern forbidden) throws IOException {
        List<String> found = new ArrayList<>();
        for (Path file : scanned()) {
            List<String> lines = Files.readAllLines(file);
            for (int i = 0; i < lines.size(); i++) {
                if (forbidden.matcher(lines.get(i)).find()) {
                    found.add(REPO_ROOT.relativize(file) + ":" + (i + 1)
                            + " → " + lines.get(i).strip());
                }
            }
        }
        return found;
    }

    private static String where(Assignment one) {
        return REPO_ROOT.relativize(one.file()) + ":" + one.line();
    }

    @Test
    @DisplayName("스캔이 아는 만큼의 대입을 찾는다")
    void findsEveryKnownAssignment() throws IOException {
        assertThat(assignments())
                .as("대입을 %d건 알고 있다. **정상적으로 늘렸으면 이 상수를 올린다.** "
                        + "줄었는데 지운 적이 없으면 스캔이 좁아진 것이다 — 그때는 위반이 "
                        + "없는 것이 아니라 안 보는 것이니, 스캔을 좁히는 쪽으로 고치지 "
                        + "말 것", KNOWN_ASSIGNMENTS)
                .hasSize(KNOWN_ASSIGNMENTS);
    }

    private static List<Assignment> assignments() throws IOException {
        List<Assignment> found = new ArrayList<>();
        for (Path file : scanned()) {
            List<String> lines = Files.readAllLines(file);
            for (int i = 0; i < lines.size(); i++) {
                collect(found, file, i + 1, lines.get(i));
            }
        }
        return found;
    }

    /**
     * <b>{@code <값>} 같은 자리표시자는 대입이 아니다.</b> 이 규칙을 <b>설명하는 산문</b>이
     * 그 형태를 쓴다 — {@code scripts/pour-batch-meta.sh} 의 근거 주석과
     * {@code docs/14} 가 <i>"호스트에서 펴서 {@code -e MYSQL_PWD=<값>} 으로 넘기면 ps 에
     * 보인다"</i> 고 적는다. 그것을 위반으로 읽으면 <b>이 검사의 근거를 지우는 고침</b>을
     * 부른다.
     */
    private static void collect(List<Assignment> into, Path file, int line, String text) {
        Matcher assignment = ASSIGNMENT.matcher(text);
        while (assignment.find()) {
            // 산문에 섞이면 조사·쉼표·백틱이 값에 붙는다 — 그것까지 값으로 읽으면
            // **금지 형태를 문서에 예시하는 것 자체가 위반**이 되어, 검사가 자기 근거를
            // 못 적게 만든다.
            String value = assignment.group(1).replaceAll("[`,.)\\]]+$", "");
            if (PLACEHOLDER.matcher(value).find()) {
                continue;
            }
            into.add(new Assignment(file, line, value));
        }
    }

    /** 경로 구분자를 편다. 형제들과 같은 관용이다({@code NoOrphanJavadocTest} 등). */
    private static String slashed(Path path) {
        return path.toString().replace('\\', '/');
    }

    /** 산문의 자리표시자. {@code <값>}·{@code <password>} 처럼 꺾쇠로 싼 것. */
    private static final Pattern PLACEHOLDER = Pattern.compile("^<[^>]+>$");

    /**
     * <b>셸이 실제로 도는 자리만 본다</b> — 문서 블록과 {@code .sh}.
     *
     * <p><b>자바는 안 본다.</b> {@code MySqlContainerConfig.executeAsRoot} 가
     * {@code "MYSQL_PWD=" + container.getPassword()} 를 쓰는데, 그 값은 <b>Testcontainers 가
     * 매 기동 새로 만드는 것</b>이라 리터럴이 아니고, 대상도 그 테스트만 쓰는 일회용
     * 컨테이너다 — 이 검사가 막는 <b>"설정을 바꾸면 깨진다"</b> 가 성립하지 않는다.
     *
     * <p>⚠️ 한때 여기 <i>"호스트 셸을 안 거치니 ps 도 없다"</i> 고 적었는데 <b>인과가
     * 틀렸다.</b> 리눅스 도커 호스트에서는 컨테이너 프로세스의 argv 가 호스트
     * 프로세스 테이블에 그대로 보인다. 그 자리가 안전한 것은 {@code env} 가
     * {@code execve} 하면서 자기 argv 를 버리기 때문이지 API 를 써서가 아니다.
     */
    private static List<Path> scanned() throws IOException {
        try (Stream<Path> docs = Files.list(REPO_ROOT.resolve("docs"));
                Stream<Path> scripts = Files.list(REPO_ROOT.resolve("scripts"));
                Stream<Path> infra = Files.walk(REPO_ROOT.resolve("infra"))) {
            return Stream.of(
                            docs.filter(path -> path.toString().endsWith(".md")),
                            scripts.filter(path -> path.toString().endsWith(".sh")),
                            infra.filter(path -> path.toString().endsWith(".sh"))
                                    .filter(Files::isRegularFile)
                                    // 형제 scannedJavaSources 와 같은 exclude 다. infra 아래
                                    // 파일 600개 중 442개가 build/ 라, 지금 .sh 가 없을 뿐
                                    // 하나 떨어지는 순간 **다른 태스크의 출력이 이 태스크의
                                    // 입력**이 된다.
                                    .filter(path -> !slashed(path).contains("/build/")))
                    .flatMap(stream -> stream)
                    .distinct()
                    .sorted()
                    .toList();
        }
    }

    /**
     * <b>스캔이 잡아야 할 것과 안 잡아야 할 것을 못 박는다.</b> 저장소 파일만으로는
     * 스캔이 조용히 좁아지거나 넓어지는 것을 못 잰다.
     */
    @Nested
    class Detection {

        @Test
        @DisplayName("컨테이너 안에서 편 값은 통과한다")
        void acceptsTheContainerForm() {
            assertThat(bad("'MYSQL_PWD=\"$MYSQL_ROOT_PASSWORD\" exec mysql -uroot'")).isZero();
            assertThat(bad("MYSQL_PWD=${MYSQL_ROOT_PASSWORD} mysql -uroot")).isZero();
        }

        @Test
        @DisplayName("리터럴은 위반이다")
        void flagsALiteral() {
            assertThat(bad("docker compose exec -T -e MYSQL_PWD=root mysql mysql -uroot")).isOne();
        }

        @Test
        @DisplayName("호스트에서 편 값도 위반이다 — ps 에 남는다")
        void flagsAHostExpansion() {
            assertThat(bad("exec -T -e MYSQL_PWD=\"${DB_ROOT_PASSWORD:-root}\" mysql")).isOne();
        }

        @Test
        @DisplayName("-e 로 넘기면 값이 무엇이든 위반이다")
        void flagsTheDockerEnvFlagRegardlessOfValue() {
            String sneaky = "docker compose exec -T -e MYSQL_PWD=\"$MYSQL_ROOT_PASSWORD\" mysql";
            assertThat(bad(sneaky))
                    .as("값만 보면 이것이 통과한다 — sh -c 를 걷어내고 이름만 남긴 모양이다")
                    .isZero();
            assertThat(ON_HOST_COMMAND_LINE.matcher(sneaky).find())
                    .as("자리를 보면 잡힌다")
                    .isTrue();
        }

        @Test
        @DisplayName("컨테이너 안 형태는 -e 규칙에 안 걸린다")
        void leavesTheContainerFormAlone() {
            assertThat(ON_HOST_COMMAND_LINE.matcher(
                    "exec -T mysql sh -c 'MYSQL_PWD=\"$MYSQL_ROOT_PASSWORD\" exec mysql'")
                    .find()).isFalse();
        }

        @Test
        @DisplayName("-p·--password 인자도 잡는다")
        void flagsANakedPassword() {
            assertThat(NAKED_PASSWORD.matcher("docker exec c mysql -uroot -proot t").find())
                    .isTrue();
            assertThat(NAKED_PASSWORD.matcher(
                    "mysql -uroot --password=\"$DB_ROOT_PASSWORD\"").find()).isTrue();
            assertThat(NAKED_PASSWORD.matcher(
                    "MYSQL_PWD=\"$MYSQL_ROOT_PASSWORD\" exec mysql -uroot -N -e \"SELECT 1\"")
                    .find())
                    .as("-N 같은 다른 짧은 옵션을 -p 로 오해하면 안 된다")
                    .isFalse();
        }

        @Test
        @DisplayName("값에 붙은 조사·백틱은 벗기고 본다")
        void stripsProsePunctuation() {
            assertThat(bad("`-e MYSQL_PWD=<값>`, 처럼 넘기면 ps 에 보인다")).isZero();
        }

        @Test
        @DisplayName("대입이 아닌 언급은 안 센다")
        void ignoresMentions() {
            assertThat(count("-p 를 인자로 주면 ps 에 남으므로 MYSQL_PWD 로 넘긴다")).isZero();
        }

        private long count(String text) {
            List<Assignment> found = new ArrayList<>();
            collect(found, Path.of("x"), 1, text);
            return found.size();
        }

        private long bad(String text) {
            List<Assignment> found = new ArrayList<>();
            collect(found, Path.of("x"), 1, text);
            return found.stream()
                    .filter(one -> !FROM_CONTAINER.matcher(one.value()).matches())
                    .count();
        }
    }
}
