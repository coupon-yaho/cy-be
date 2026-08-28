// 경로 모양을 바꿔 관문을 우회할 수 있는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>관문은 "맞는 토큰을 막지 않는가" 보다 "틀린 요청을 통과시키지 않는가" 가 어렵다.</b>
 * {@link AdminTokenFilterTest} 가 앞쪽을 재고, 여기는 <b>경로 모양을 비틀어</b> 뒤쪽을 잰다.
 *
 * <p><b>단언은 하나다 — 어느 모양도 2xx 를 내면 안 된다.</b> 401(관문이 잡음)이든
 * 404·400(라우팅이 안 됨)이든 상관없다. 상태 코드를 모양마다 못 박으면 스프링이 라우팅을
 * 조금만 바꿔도 <b>보안과 무관한 이유로</b> 빨개진다.
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
                "batch.admin.auth.required=true",
                "batch.admin.auth.token=" + AdminTokenBypassTest.SECRET
        })
@Import(MySqlContainerConfig.class)
class AdminTokenBypassTest {

    static final String SECRET = "bypass-test-secret";

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();

    @LocalServerPort
    private int port;

    /**
     * 슬래시 중복·상대 경로·대소문자·path parameter·인코딩·앞 슬래시 둘. 서블릿 컨테이너와
     * 스프링이 경로를 정규화하는 지점이 서로 달라서, 그 틈이 관문과 라우팅을 <b>엇갈리게</b>
     * 만들 수 있는 자리들이다.
     */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "/api/v1/admin/verify/runs/running",
            "/api/v1/admin/verify/runs/running/",
            "/api/v1/admin//verify/runs/running",
            "/api/v1/./admin/verify/runs/running",
            "/api/v1/admin/../admin/verify/runs/running",
            "/API/V1/ADMIN/verify/runs/running",
            "/api/v1/admin/verify/runs/running;a=b",
            "/api/v1/admin/verify/runs/running%2f",
            "/api/v1/admin/verify/runs/running?x=1",
            "//api/v1/admin/verify/runs/running",
            // ⚠️ 아래 넷은 **실제로 뚫렸던 자리**다. getRequestURI() 가 디코딩 안 된 원문이라
            //    한 글자만 퍼센트 인코딩하면 관문의 접두사 검사가 거짓이 되어 건너뛰는데,
            //    톰캣과 스프링은 디코딩된 경로로 매칭해 컨트롤러까지 간다 — 실측 200 이었다.
            "/api/v1/%61dmin/verify/runs/running",
            "/api/v1/adm%69n/verify/runs/running",
            "/api/v1/admi%6e/verify/runs/running",
            "/%61pi/v1/admin/verify/runs/running",
            "/api/v1/admin/%76erify/runs/running"
    })
    @DisplayName("토큰 없이 어떤 경로 모양으로도 통과하지 못한다")
    void noShapeReachesTheControllerWithoutToken(String path) throws Exception {
        int status = status("http://127.0.0.1:" + port + path);

        assertThat(status)
                .as("2xx 가 나오면 관문을 지나 컨트롤러가 답한 것이다 — 우회다. 받은 값=%d",
                        status)
                .matches(code -> code < 200 || code > 299);
    }

    private static int status(String url) {
        try {
            return CLIENT.send(HttpRequest.newBuilder(URI.create(url))
                            .GET().timeout(Duration.ofSeconds(10)).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode();
        } catch (IllegalArgumentException | java.io.IOException e) {
            // 클라이언트가 URI 를 거절하거나 연결이 끊긴 것은 "통과 안 함" 이다.
            return -1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * <b>여기도 실제로 뚫렸던 자리다.</b> {@code getRequestURI()} 가 컨텍스트 경로를 <b>포함</b>해서,
     * {@code server.servlet.context-path} 를 주는 순간 관문의 {@code startsWith} 가 거짓이 되고
     * 필터가 통째로 건너뛰어졌다 — 실측에서 <b>200</b> 이 나왔다. 지금 이 저장소는 컨텍스트
     * 경로를 안 쓰지만, 붙이는 날 <b>조용히</b> 열리므로 그 조합을 못 박아 둔다.
     */
    @Nested
    @DisplayName("컨텍스트 경로를 붙여도")
    @SpringBootTest(
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {
                    "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
                    "spring.batch.job.enabled=false",
                    "batch.scheduling.enabled=false",
                    "batch.metrics.expire-pending-initial-delay-ms=3600000",
                    "batch.metrics.run-refresh-ms=120000",
                    "management.server.port=0",
                    "server.servlet.context-path=/batch",
                    "batch.admin.auth.required=true",
                    "batch.admin.auth.token=" + SECRET
            })
    class UnderContextPath {

        @LocalServerPort
        private int nestedPort;

        @org.junit.jupiter.api.Test
        @DisplayName("토큰 없이는 못 지난다")
        void stillRequiresTheToken() {
            assertThat(status("http://127.0.0.1:" + nestedPort
                    + "/batch/api/v1/admin/verify/runs/running"))
                    .as("컨텍스트 경로를 벗기고 판정하지 않으면 여기가 200 이 된다")
                    .isEqualTo(401);
        }
    }
}
