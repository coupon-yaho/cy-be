package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Bean;

import com.kafkick.storage.db.config.ObservationHealthConfig;

/**
 * 관측 풀 장애가 서비스 가용성으로 번지지 않는다는 계약을, <b>계약이 실제로 걸쳐 있는 두 곳을 이어서</b>
 * 확인한다.
 *
 * <ul>
 *   <li>코드 상수 — storage 의 {@link ObservationHealthConfig#OBSERVATION_DOWN}
 *   <li>설정 파일 — api 의 {@code management.yml.example} 의 {@code status.order} 와 obs 그룹 매핑
 * </ul>
 *
 * <p>둘을 따로 검증하는 테스트 두 개로는 계약을 지킬 수 없다. 상수 이름만 바꿔도, 순서 목록에서만
 * 빼도, 각자의 테스트는 통과하면서 합산 상태는 무너진다. 그래서 여기서는 상수를 그대로 쓰는 기여자를
 * 띄우고 템플릿을 그대로 읽어 <b>HTTP 상태 코드</b>까지 간다 — 로드밸런서가 실제로 보는 것이 그것이다.
 *
 * <p>DB 는 띄우지 않는다. 확인 대상은 "풀이 죽었을 때 무엇이 노출되는가" 이지 풀 자체가 아니라서,
 * 기여자를 가짜로 두는 편이 실패를 확정적으로 만든다.
 *
 * <p>읽는 파일이 {@code .yml} 이 아니라 {@code .yml.example} 인 이유 — 실제 {@code management.yml} 은
 * gitignore 대상이라 신규 클론과 CI 에 없다. {@code [.yml]} 은 확장자 힌트다.
 */
@SpringBootTest(
        classes = ObservationHealthContractTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.config.import=classpath:management.yml.example[.yml]",
                "management.server.port=0"
        })
class ObservationHealthContractTest {

    private static final AtomicReference<Status> MAIN_POOL = new AtomicReference<>(Status.UP);

    private static final AtomicReference<Status> OBSERVATION_POOL =
            new AtomicReference<>(ObservationHealthConfig.OBSERVATION_DOWN);

    @LocalManagementPort
    int managementPort;

    @BeforeEach
    void resetPools() {
        MAIN_POOL.set(Status.UP);
        OBSERVATION_POOL.set(ObservationHealthConfig.OBSERVATION_DOWN);
    }

    /**
     * 관측 도구가 측정 대상을 바꾸면 안 된다는 이 티켓의 전제가, 가용성 쪽에서 성립하는지 보는 자리다.
     * 깨지면 발급 API 는 멀쩡한데 인스턴스가 로드밸런서에서 빠진다.
     */
    @Test
    @DisplayName("관측 풀이 죽어도 합산 헬스는 UP 이고 200 이다")
    void observationFailureNeverTakesTheInstanceOut() throws Exception {
        HttpResponse<String> response = get("/actuator/health");

        assertThat(response.statusCode()).isEqualTo(200);
        // 200 만 보면 부족하다 — 합산 상태가 OBSERVATION_DOWN 으로 바뀌어도 기본 매핑에 없어서 200 이다.
        assertThat(response.body()).contains("\"status\":\"UP\"");
    }

    /**
     * 반대 방향 실패 — 관측을 지키려는 설정이 운영 장애를 숨기면 그게 더 큰 사고다.
     * 전역 {@code http-mapping} 을 한 줄이라도 넣으면 기본 매핑이 교체돼 여기가 200 으로 바뀐다.
     */
    @Test
    @DisplayName("운영 풀이 죽으면 503 이다 — 관측을 지키는 설정이 이걸 못 가려야 한다")
    void mainPoolFailureStillFailsTheHealthCheck() throws Exception {
        MAIN_POOL.set(Status.DOWN);

        assertThat(get("/actuator/health").statusCode()).isEqualTo(503);
    }

    /** 감시에서 빼기만 하면 비밀번호 오타 배포가 대시보드 첫 조회까지 안 드러난다. 그 구멍을 막는 그룹이다. */
    @Test
    @DisplayName("obs 그룹으로 조회하면 관측 풀 장애가 503 으로 보인다 — 경보를 걸 수 있어야 한다")
    void observationFailureIsVisibleOnItsOwnGroup() throws Exception {
        assertThat(get("/actuator/health/obs").statusCode()).isEqualTo(503);
    }

    /** 관측 풀이 멀쩡하면 그룹도 통과해야 한다. 늘 503 이면 경보가 무의미하다. */
    @Test
    @DisplayName("관측 풀이 살아 있으면 obs 그룹은 200 이다")
    void observationGroupPassesWhenThePoolIsHealthy() throws Exception {
        OBSERVATION_POOL.set(Status.UP);

        assertThat(get("/actuator/health/obs").statusCode()).isEqualTo(200);
    }

    /**
     * 관측 풀 실패 details 에는 원본 예외 메시지가 담긴다 — 계정명·JDBC URL 이 그대로 실린다.
     * 응답에 그게 안 실리는 것은 {@code show-details: never} 와 기여자 쪽 details 제거, 둘 다에 걸려 있다.
     */
    @Test
    @DisplayName("헬스 응답에 details 가 실리지 않는다")
    void healthResponseCarriesNoDetails() throws Exception {
        assertThat(get("/actuator/health").body()).doesNotContain("details");
        assertThat(get("/actuator/health/obs").body()).doesNotContain("details");
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:" + managementPort + path)).build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class
    })
    static class TestApp {

        /** 이름이 {@code db} 여야 기본 기여자와 같은 자리를 차지한다. */
        @Bean
        HealthIndicator dbHealthContributor() {
            return () -> Health.status(MAIN_POOL.get()).build();
        }

        /**
         * 이름이 {@code obsDb} 여야 management.yml 의 {@code group.obs.include} 가 잡는다.
         * 상태는 storage 의 상수를 그대로 쓴다 — 이 참조가 코드와 설정 파일을 잇는 지점이다.
         */
        @Bean
        HealthIndicator obsDbHealthContributor() {
            return () -> Health.status(OBSERVATION_POOL.get()).build();
        }
    }
}
