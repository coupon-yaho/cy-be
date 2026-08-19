// include 가 넓어져도 위험한 문이 닫혀 있는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>{@code exclude} 는 지금 아무 일도 하지 않습니다.</b> {@code include} 가 화이트리스트라
 * 목록에 없는 엔드포인트는 애초에 열리지 않습니다 — {@code exclude} 를 통째로 지워도
 * {@link ActuatorExposureTest} 는 초록입니다(돌연변이로 실측).
 *
 * <p>그러면 {@code exclude} 를 왜 두는가. <b>누가 {@code include} 를 {@code *} 로 넓히는 날</b>을
 * 위해서입니다. 관측 항목을 하나 더 보려고 별표로 바꾸는 것은 실제로 흔한 변경이고,
 * 그 순간 {@code exclude} 가 유일한 방어가 됩니다.
 *
 * <p>그 상황을 여기서 재현합니다. 이 클래스가 없으면 {@code exclude} 는
 * <b>지워도 아무것도 빨개지지 않는 줄</b>이 됩니다.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.batch.job.enabled=false",
                "batch.scheduling.enabled=false",
                "management.server.port=0",
                // 사고를 재현한다. 운영 설정은 화이트리스트다.
                "management.endpoints.web.exposure.include=*",
                "management.endpoints.web.exposure.exclude=env,configprops,beans,heapdump,loggers,threaddump,mappings,scheduledtasks,conditions,flyway,sbom"
        })
@Import(MySqlContainerConfig.class)
class ActuatorWildcardExposureTest {

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @LocalManagementPort
    private int managementPort;

    @Test
    @DisplayName("include 가 * 여도 위험한 열하나는 닫혀 있다")
    void excludeStillClosesThemUnderWildcard() throws Exception {
        // 별표가 실제로 넓혔는지 먼저 본다. 안 넓어졌으면 아래 단언 전부가 헛돈다.
        //
        // health 로는 이것을 못 본다 — 스프링 기본 노출이 health 하나라, include 가 통째로
        // 안 먹어도 200 이다. metrics 는 기본 미노출이라 별표가 먹었을 때만 열린다.
        assertThat(get("metrics").statusCode())
                .as("별표가 안 넓어졌다. 아래 404 들이 exclude 덕인지 알 수 없어진다")
                .isEqualTo(200);

        // include 가 * 일 때 실제로 열리는 것을 재 보니 16개였다. 그중 위험한 것 전부다.
        // loggers 는 POST 로 런타임 로그 레벨을 바꾸는 **쓰기** 엔드포인트다.
        for (String path : new String[] {
                "env", "configprops", "beans", "heapdump",
                "loggers", "threaddump", "mappings", "scheduledtasks",
                "conditions", "flyway", "sbom"}) {
            assertThat(get(path).statusCode())
                    .as("/actuator/%s 가 열려 있다. exclude 가 지워졌거나 안 먹는다", path)
                    .isEqualTo(404);
        }
    }

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        return CLIENT.send(
                HttpRequest.newBuilder(
                                URI.create("http://localhost:" + managementPort + "/actuator/" + path))
                        .timeout(Duration.ofSeconds(10))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
