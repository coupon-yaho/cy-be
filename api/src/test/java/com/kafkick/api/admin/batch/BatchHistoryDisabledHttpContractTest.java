package com.kafkick.api.admin.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>관측을 끈 환경에서 이 경로가 무엇을 돌려주는가.</b>
 *
 * <p>이력 조회는 관측 전용 풀로만 읽으므로, 관측을 끄면 읽을 원천이 없다. 그때 응답이
 * <b>404 면 안 된다</b> — 화면이 <i>"그런 기능이 없다"</i> 와 <i>"관측이 꺼져 있다"</i> 를
 * 구분하지 못한다. 앞은 배포가 잘못된 것이고 뒤는 사람이 스위치를 내린 것이라 조치가 다르다.
 *
 * <p>빈 목록(200)도 안 된다. 그건 <i>"배치가 한 번도 안 돌았다"</i> 로 읽힌다 —
 * 이 티켓이 없애려던 바로 그 오해다.
 *
 * <p>그래서 <b>503 + 전용 코드</b>다. 상태 코드가 <i>"지금은 못 준다"</i> 를 말하고,
 * 코드가 <i>"왜"</i> 를 말한다.
 */
@SpringBootTest(classes = com.kafkick.ApiApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "observation.datasource.enabled=false",
                "observation.domain-gauge.enabled=false",
                // api 의 management.yml 은 헬스 그룹 obs 가 obsDb 기여자를 지목하고
                // 멤버십 검증을 켜 둔다. 그래서 **api 는 커밋된 설정 그대로는 관측을 끈 채
                // 뜨지 못한다** — 그 검증이 먼저 기동을 막는다(실측).
                //
                // 그럼 503 경로는 언제 쓰이나. 설정을 갈아끼워 관측 키가 아예 없는 컨텍스트다 —
                // KafkaLayerWiringTest 가 그 형태이고, 거기서 어댑터 빈이 없다.
                // 여기서는 그 상태를 만들려고 검증만 내린다. 운영 설정은 건드리지 않는다.
                "management.endpoint.health.validate-group-membership=false"
        })
@Import(MySqlContainerConfig.class)
class BatchHistoryDisabledHttpContractTest {

    @Value("${local.server.port}")
    int port;

    private HttpResponse<String> call() throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(
                                "http://localhost:" + port + "/api/v1/admin/batch-executions?limit=50"))
                        .header(MemberRequestHeaders.MEMBER_ID, "812934")
                        .header("X-User-Role", "ADMIN")
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @DisplayName("관측을 끄면 404 가 아니라 503 과 전용 코드로 답한다")
    void respondsServiceUnavailableWithDedicatedCode() throws Exception {
        HttpResponse<String> response = call();

        assertThat(response.statusCode())
                .as("404 면 '기능 없음' 과 구분되지 않는다")
                .isEqualTo(503);
        assertThat(response.body())
                .as("왜 못 주는지가 코드로 나와야 한다")
                .contains("ADMIN-003");
    }
}
