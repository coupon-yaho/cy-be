// 관리자 토큰 관문을 켠 채로 재고, 켠 채 토큰이 비면 안 뜨는 것까지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>테스트 설정이 이 관문을 한 곳에서 끈다.</b> 그래서 <b>아무 테스트도 켠 상태를 안 지나고</b>,
 * 관문을 실수로 지워도 저장소가 통째로 초록이다 — 형제 {@code DefaultZoneGuardTest} 가
 * 같은 자리에서 같은 이유로 켠 상태를 직접 만든다.
 *
 * <p>세 방향을 잰다: <b>토큰이 없으면 막히는가</b> · <b>맞으면 지나는가</b> ·
 * <b>켠 채 토큰이 비면 안 뜨는가</b>. 셋 중 하나만 빠져도 관문이 있는 척이 된다.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
                "spring.batch.job.enabled=false",
                "batch.scheduling.enabled=false",
                "batch.metrics.expire-pending-initial-delay-ms=3600000",
                "batch.metrics.run-refresh-ms=120000",
                "management.server.port=0",
                AdminTokenFilterTest.REQUIRED_ON,
                AdminTokenFilterTest.TOKEN_IS
        })
@Import(MySqlContainerConfig.class)
class AdminTokenFilterTest {

    static final String SECRET = "demo-token-1234567890";
    static final String REQUIRED_ON = AdminTokenConfig.REQUIRED + "=true";
    static final String TOKEN_IS = AdminTokenConfig.TOKEN + "=" + SECRET;

    /** 부수효과가 없는 조회를 고른다 — 막히는 것을 재는데 지나면 잡이 뜨면 곤란하다. */
    private static final String PATH = "/api/v1/admin/verify/runs/running";

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @LocalServerPort
    private int port;

    @Test
    @DisplayName("토큰이 없으면 401 이고, 어느 쪽으로 틀렸는지는 안 알려 준다")
    void rejectsWithoutToken() throws Exception {
        HttpResponse<String> response = call(null);

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body())
                .as("있는데 틀린 것과 아예 없는 것을 응답에서 가르면 그 자체가 힌트가 된다")
                .doesNotContain("없", "틀");
        assertThat(response.body()).contains("BATCH-ADMIN-401");
    }

    @Test
    @DisplayName("틀린 토큰도 401 이다")
    void rejectsWrongToken() throws Exception {
        assertThat(call(SECRET + "x").statusCode()).isEqualTo(401);
        assertThat(call(SECRET.substring(0, SECRET.length() - 1)).statusCode()).isEqualTo(401);
    }

    @Test
    @DisplayName("응답 어디에도 토큰이 안 실린다")
    void neverEchoesTheSecret() throws Exception {
        // 헤더 값은 ASCII 만 실린다(JDK 클라이언트가 거절한다). 길이도 다르게 둔다.
        HttpResponse<String> response = call("totally-unrelated-value");

        assertThat(response.body()).doesNotContain(SECRET);
        assertThat(response.headers().map().toString()).doesNotContain(SECRET);
    }

    @Test
    @DisplayName("맞는 토큰은 지나간다")
    void acceptsTheToken() throws Exception {
        HttpResponse<String> response = call(SECRET);

        assertThat(response.statusCode())
                .as("관문을 지나면 컨트롤러가 답한다 — 401 이 아니어야 한다")
                .isNotEqualTo(401);
        assertThat(response.statusCode()).isLessThan(500);
    }

    /**
     * <b>프리플라이트는 지나야 한다.</b> 브라우저는 거기에 사용자 헤더를 안 실으므로, 막으면
     * <b>본 요청이 아예 안 나가고</b> 관제 버튼이 "CORS 오류" 로만 보인다 — 원인이 토큰이라는
     * 것을 아무도 못 찾는다.
     */
    @Test
    @DisplayName("프리플라이트는 토큰 없이도 지난다")
    void letsPreflightThrough() throws Exception {
        HttpRequest preflight = HttpRequest.newBuilder(URI.create(base() + PATH))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .header("Origin", "http://localhost:3000")
                .header("Access-Control-Request-Method", "GET")
                .timeout(Duration.ofSeconds(10))
                .build();

        HttpResponse<String> response = CLIENT.send(preflight,
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isNotEqualTo(401);
        assertThat(response.headers().firstValue("access-control-allow-origin"))
                .contains("http://localhost:3000");
    }

    private HttpResponse<String> call(String token) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base() + PATH))
                .GET().timeout(Duration.ofSeconds(10));
        if (token != null) {
            builder.header(AdminTokenFilter.HEADER, token);
        }
        return CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private String base() {
        return "http://127.0.0.1:" + port;
    }

    /**
     * <b>"켰다고 믿는데 안 켜진" 상태를 막는다.</b> 환경변수를 빠뜨린 배포가 조용히 무방비가
     * 되는 것이 이 관문이 막으려던 바로 그 실패다.
     */
    @Nested
    @DisplayName("켠 채 토큰이 비면")
    class BlankToken {

        @Test
        @DisplayName("기동을 거절하고, 고치는 법과 일부러 여는 법을 둘 다 말한다")
        void refusesToStart() {
            assertThatThrownBy(() -> new AdminTokenConfig().adminTokenFilter(
                    true, "  ", new io.micrometer.core.instrument.simple.SimpleMeterRegistry()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("BATCH_ADMIN_TOKEN")
                    .hasMessageContaining("BATCH_ADMIN_AUTH_REQUIRED=false");
        }

        @Test
        @DisplayName("끈 상태라면 토큰이 비어도 뜬다 — 대신 필터를 안 단다")
        void staysOpenWhenExplicitlyDisabled() {
            var registration = new AdminTokenConfig().adminTokenFilter(
                    false, "", new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

            assertThat(registration.isEnabled())
                    .as("끈 상태에서 관문이 달리면 토큰 없이 전부 401 이 되어 배치가 먹통이 된다")
                    .isFalse();
            // **isEnabled 만 보면 부족했다.** 필터를 안 넣어 뒀더니 등록 시점 검사에 걸려
            // 톰캣이 "'filter' must not be null" 로 죽었고, 이 단언은 그때도 통과했다 —
            // 컨텍스트를 띄우는 테스트 102개가 대신 죽고 나서야 드러났다.
            assertThat(registration.getFilter())
                    .as("끈 상태에도 인스턴스는 있어야 컨텍스트가 뜬다")
                    .isNotNull();
        }
    }
}
