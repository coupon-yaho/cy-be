// JDBC 타임아웃 가드의 갈래를 컨테이너 없이 잽니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>기동을 막는 가드는 두 방향으로 다 틀릴 수 있다.</b> 거짓 양성은 멀쩡한 배포를 못 뜨게
 * 하고, 거짓 음성은 이 가드가 있으나 마나 하게 만든다. 형제 가드들
 * ({@code SchemaPresenceGuardTest} · {@code RunningJobProbeSettingsTest} ·
 * {@code SchedulerPoolGuardTest})이 전부 그 둘을 재는데 이 가드만 없었다.
 *
 * <p>스프링 컨텍스트를 안 띄운다 — 생성자가 검사의 전부라 직접 부르면 된다.
 */
class DataSourceTimeoutGuardTest {

    private static final long VERIFY = 600_000;
    private static final long EXPIRE = 120_000;
    private static final long CLEANUP = 120_000;

    /** {@code getJdbcUrl()} 만 가진 최소 구현. 가드는 그것만 읽는다. */
    private static DataSource urlOf(String url) {
        return (DataSource) java.lang.reflect.Proxy.newProxyInstance(
                DataSourceTimeoutGuardTest.class.getClassLoader(),
                new Class<?>[] {DataSource.class, HasJdbcUrl.class},
                (proxy, method, args) -> "getJdbcUrl".equals(method.getName()) ? url : null);
    }

    /** 프록시가 {@code getJdbcUrl} 을 갖게 하는 최소 인터페이스. */
    public interface HasJdbcUrl {
        String getJdbcUrl();
    }

    private static DataSourceTimeoutGuard guard(String url, boolean required) {
        return new DataSourceTimeoutGuard(urlOf(url), required, VERIFY, EXPIRE, CLEANUP,
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

    @Test
    @DisplayName("배포되는 URL 이 그대로 통과한다")
    void acceptsTheShippedUrl() throws IOException {
        String shipped = shippedUrl();

        assertThatCode(() -> new DataSourceTimeoutGuard(
                urlOf(shipped), true, VERIFY, EXPIRE, CLEANUP,
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry()))
                .as("storage.yml.example 의 URL 과 이 가드가 갈리면 배포가 못 뜬다. "
                        + "그 둘을 여기서 묶는다 — 값을 고치는 사람이 한쪽만 고치면 빨개진다")
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("socketTimeout 이 가장 긴 Step 데드라인보다 작으면 거절한다")
    void rejectsWhenSocketTimeoutIsSmallerThanTheLongestStep() {
        assertThatThrownBy(() -> guard(
                "jdbc:mysql://h/db?connectTimeout=2000&socketTimeout=" + VERIFY, true))
                .isInstanceOf(IllegalStateException.class)
                .as("어느 키를 함께 올려야 하는지 말해야 한다 — 숫자만 주면 사람이 못 찾는다")
                .hasMessageContaining("batch.verify.step-timeout-ms")
                .hasMessageContaining("DB_SOCKET_TIMEOUT_MS");
    }

    /**
     * <b>{@code 0} 은 "작다" 가 아니라 "무제한" 이다</b>(Connector/J 규약). 거절하는 것은
     * 맞지만 <b>이유가 정반대</b>라 문구가 갈려야 한다 — 같은 메시지를 내면 운영자가
     * 값을 더 키우려 든다.
     */
    @Test
    @DisplayName("socketTimeout=0 은 무제한이라 거절하되 이유를 따로 말한다")
    void rejectsUnboundedSocketTimeoutWithItsOwnReason() {
        assertThatThrownBy(() -> guard(
                "jdbc:mysql://h/db?connectTimeout=2000&socketTimeout=0", true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("무제한")
                .as("'작다' 로 읽히면 안 된다")
                .hasMessageNotContaining("커야 합니다");
    }

    /** {@code connectTimeout=0} 도 무제한이다 — {@code socketTimeout} 과 같은 함정이다. */
    @Test
    @DisplayName("connectTimeout=0 은 무제한이라 거절한다")
    void rejectsUnboundedConnectTimeout() {
        assertThatThrownBy(() -> guard(
                "jdbc:mysql://h/db?connectTimeout=0&socketTimeout=660000", true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connectTimeout=0")
                .hasMessageContaining("무제한");
    }

    @Test
    @DisplayName("socketTimeout 이 없으면 거절한다")
    void rejectsWhenSocketTimeoutIsMissing() {
        assertThatThrownBy(() -> guard("jdbc:mysql://h/db?connectTimeout=2000", true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("socketTimeout");
    }

    @Test
    @DisplayName("connectTimeout 이 없으면 거절한다")
    void rejectsWhenConnectTimeoutIsMissing() {
        assertThatThrownBy(() -> guard("jdbc:mysql://h/db?socketTimeout=660000", true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connectTimeout");
    }

    /**
     * <b>탈출구.</b> 이 가드가 막으려는 사고를 가드 자신이 일으킬 수 있는 자리라, 끄는
     * 손잡이가 없으면 되돌릴 방법이 없다({@code SchemaPresenceGuard} 와 같은 판단).
     */
    @Test
    @DisplayName("거절을 끄면 뜬다")
    void startsWhenEnforcementIsOff() {
        assertThatCode(() -> guard("jdbc:mysql://h/db", false)).doesNotThrowAnyException();
    }

    /** URL 을 못 읽는 구현이면 검사를 건너뛴다 — 여기서 막으면 풀을 바꾸는 날 안 뜬다. */
    @Test
    @DisplayName("JDBC URL 을 못 읽으면 건너뛴다")
    void skipsWhenTheUrlCannotBeRead() {
        DataSource opaque = (DataSource) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {DataSource.class},
                (proxy, method, args) -> null);

        assertThatCode(() -> new DataSourceTimeoutGuard(opaque, true, VERIFY, EXPIRE, CLEANUP,
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry()))
                .doesNotThrowAnyException();
    }

    /** {@code .example} 에서 URL 을 읽어 온다 — 그 파일이 배포되는 원본이다. */
    private static String shippedUrl() throws IOException {
        Path example = Path.of("..", "storage", "src", "main", "resources",
                "storage.yml.example");
        String text = Files.readString(example, StandardCharsets.UTF_8);
        String line = text.lines()
                .filter(candidate -> candidate.trim().startsWith("url: jdbc:mysql"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("storage.yml.example 에 url 이 없습니다"));
        String raw = line.substring(line.indexOf("jdbc:mysql")).trim();
        // ${DB_CONNECT_TIMEOUT_MS:2000} 같은 플레이스홀더를 기본값으로 푼다.
        return raw.replaceAll("\\$\\{[A-Z_]+:([^}]*)\\}", "$1");
    }

    @Test
    @DisplayName("배포 URL 의 두 값이 실제로 박혀 있다 — 플레이스홀더만 있고 기본값이 없으면 안 된다")
    void shippedUrlCarriesBothTimeouts() throws IOException {
        assertThat(shippedUrl())
                .contains("connectTimeout=")
                .contains("socketTimeout=");
    }
}
