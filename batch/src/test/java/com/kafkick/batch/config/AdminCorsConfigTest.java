// 관제 오리진이 실제로 브라우저에서 읽히는지 HTTP 로 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>CORS 는 코드를 읽어서는 확인이 안 된다.</b> 헤더가 붙는지는 스프링이 프리플라이트를
 * 어떻게 처리하느냐에 달려 있고, 경로 패턴 한 글자만 어긋나도 <b>조용히 아무 헤더도 안 붙는다</b> —
 * 그때 증상은 브라우저 콘솔에서만 보인다. 그래서 실제 HTTP 로 묻는다.
 *
 * <p><b>관리 포트도 함께 잰다.</b> {@code management.server.port} 가 다르면 액추에이터가 별도
 * 서블릿 컨텍스트에서 뜨므로 이 설정이 안 붙는다 — 그 사실을 주석으로만 두면 다음 사람이
 * <i>"관리 포트도 열렸겠지"</i> 로 읽는다.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
                "spring.batch.job.enabled=false",
                "batch.scheduling.enabled=false",
                "batch.metrics.expire-pending-initial-delay-ms=3600000",
                "batch.metrics.run-refresh-ms=120000",
                "management.server.port=0"
        })
@TestPropertySource(properties =
        AdminCorsConfig.ORIGINS + "=http://localhost:3000,http://127.0.0.1:5173")
@Import(MySqlContainerConfig.class)
class AdminCorsConfigTest {

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @LocalServerPort
    private int port;

    @LocalManagementPort
    private int managementPort;

    @Test
    @DisplayName("허용한 오리진은 관리자 API 를 읽을 수 있다")
    void allowsConfiguredOrigin() throws Exception {
        HttpResponse<String> preflight = preflight(port,
                "/api/v1/admin/verify/runs/running", "http://localhost:3000", "GET");

        assertThat(preflight.statusCode()).isLessThan(400);
        assertThat(preflight.headers().firstValue("access-control-allow-origin"))
                .as("이 헤더가 없으면 브라우저가 응답 본문을 못 읽는다 — 관제 버튼이 조용히 실패한다")
                .contains("http://localhost:3000");
    }

    @Test
    @DisplayName("트리거는 POST 라 그 메서드도 허용돼야 한다")
    void allowsPostForTrigger() throws Exception {
        HttpResponse<String> preflight = preflight(port,
                "/api/v1/admin/verify", "http://127.0.0.1:5173", "POST");

        assertThat(preflight.headers().firstValue("access-control-allow-methods")
                .orElse("")).contains("POST");
    }

    @Test
    @DisplayName("안 적은 오리진은 안 열린다")
    void rejectsUnknownOrigin() throws Exception {
        HttpResponse<String> preflight = preflight(port,
                "/api/v1/admin/verify/runs/running", "http://evil.example.com", "GET");

        assertThat(preflight.headers().firstValue("access-control-allow-origin"))
                .as("기본값을 * 로 두면 여기가 통과한다 — 그러면 오리진 목록이 장식이 된다")
                .isEmpty();
    }

    @Test
    @DisplayName("관리 포트에는 안 붙는다 — 별도 컨텍스트다")
    void doesNotLeakToManagementPort() throws Exception {
        HttpResponse<String> preflight = preflight(managementPort,
                "/actuator/prometheus", "http://localhost:3000", "GET");

        assertThat(preflight.headers().firstValue("access-control-allow-origin"))
                .as("지표 포트는 내부망 전용이다(docs/14) — 브라우저에 열 이유가 없다")
                .isEmpty();
    }

    private static HttpResponse<String> preflight(int atPort, String path, String origin,
            String method) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + atPort + path))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .header("Origin", origin)
                .header("Access-Control-Request-Method", method)
                .timeout(Duration.ofSeconds(10))
                .build();
        return CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
