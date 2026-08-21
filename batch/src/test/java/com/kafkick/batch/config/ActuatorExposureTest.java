// 관측 통로가 실제로 열리고 위험한 문이 실제로 닫히는지 서버를 띄워 확인합니다.
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
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>{@link ResolvedBatchConfigTest} 는 파일이 무엇을 말하는지만 봅니다.</b>
 * 그것과 "그 말대로 실제로 막힌다" 사이에는 검증되지 않은 가정이 하나 있습니다 —
 * {@code exclude} 가 정말 그 엔드포인트를 닫는지, {@code include} 에 없는 것이 정말 안 열리는지.
 *
 * <p>여기서는 서버를 띄워 <b>응답 코드</b>를 봅니다. 문자열 grep 도, 해석된 프로퍼티도
 * 이것까지는 말해 주지 못합니다.
 *
 * <p><b>실제 설정 파일을 읽습니다.</b> 한때 노출 목록을 이 클래스에 복붙하고
 * <i>"어긋나면 {@code ResolvedBatchConfigTest} 가 잡는다"</i> 고 적어 뒀는데 <b>거짓이었습니다</b> —
 * 그 클래스는 파일만 보고, 이 테스트가 자기 {@code properties} 에 뭘 적었는지는 아무도 안 봅니다.
 * 실제로 {@code exclude} 가 4개와 11개로 갈려 있었습니다. 파일을 직접 읽으면 그 틈이 없습니다.
 *
 * <p>포트만 덮습니다. 파일의 9090·9091 을 그대로 쓰면 이미 뜬 컨테이너와 부딪히고,
 * 그 실패는 설정과 아무 상관이 없습니다.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                // 실제 설정 파일을 읽는다. 노출 목록을 여기 복붙하면 파일과 어긋나도
                // 아무도 안 잡는다 — 실제로 exclude 가 4개와 11개로 갈려 있었다.
                "spring.config.location=classpath:/resolved/application.yml",
                "spring.batch.job.enabled=false",
                "batch.scheduling.enabled=false",
                // 포트만 덮는다. 파일의 9090·9091 을 그대로 쓰면 이미 뜬 컨테이너와 부딪히고,
                // 그 실패는 설정과 아무 상관이 없다.
                "server.port=0",
                "management.server.port=0"
        })
@Import(MySqlContainerConfig.class)
class ActuatorExposureTest {

    @LocalManagementPort
    private int managementPort;

    @LocalServerPort
    private int serverPort;

    /**
     * <b>열린 것의 집합 전체를 못 박습니다.</b> 개별 엔드포인트를 하나씩 확인하면
     * <i>열려야 하는 것이 안 열린 경우</i>와 <i>안 열려야 하는 것이 열린 경우</i> 중
     * 후자를 놓칩니다 — 목록에 없는 이름은 아무도 안 물어보기 때문입니다.
     *
     * <p>실측한 여섯입니다. 엔드포인트는 셋이고 {@code health}·{@code metrics} 는
     * 경로 변형이 하나씩 더 붙습니다. {@code self} 는 인덱스 자신입니다.
     *
     * <p>스프링이 판올림에서 엔드포인트를 자동 노출하기 시작해도 여기서 잡힙니다.
     * {@code exclude} 목록으로는 그것을 못 잡습니다 — 이름을 모르니 적을 수가 없습니다.
     */
    @Test
    @DisplayName("관리 포트에 열린 것은 정확히 여섯 — 엔드포인트 셋과 그 경로 변형")
    void exactlyTheThreeEndpointsAreExposed() throws Exception {
        HttpResponse<String> index = ActuatorProbe.get(managementPort, "/actuator");

        assertThat(index.statusCode()).isEqualTo(200);
        assertThat(ActuatorProbe.linkNamesOf(index.body()))
                .containsExactlyInAnyOrder(
                        "self", "health", "health-path",
                        "metrics", "metrics-requiredMetricName", "prometheus");
    }

    /**
     * 관제가 이 통로로 긁습니다. 열려 있지 않으면 지표가 통째로 안 나가는데,
     * batch JVM 에서만 나오는 값이라 대신 만들어 줄 곳이 없습니다.
     */
    @Test
    @DisplayName("관리 포트에서 prometheus 가 열린다")
    void prometheusIsServedOnTheManagementPort() throws Exception {
        HttpResponse<String> response = ActuatorProbe.get(managementPort, "/actuator/prometheus");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body())
                .as("micrometer-registry-prometheus 가 빠지면 200 이지만 본문이 비어 있다")
                .contains("jvm_memory_used_bytes");
    }

    /**
     * {@code /actuator/env} 는 환경변수를 그대로 내보내고, 이 저장소는 AES 키를 환경변수로
     * 주입합니다. 노출되면 그 자체가 유출입니다.
     *
     * <p><b>다만 지금 이것을 막는 것은 {@code exclude} 가 아니라 {@code include} 입니다</b> —
     * 화이트리스트라 목록에 없으면 애초에 열리지 않습니다. {@code exclude} 를 지워도 이 테스트는
     * 통과합니다(실측). {@code exclude} 를 지키는 것은 {@link ActuatorWildcardExposureTest} 입니다.
     */
    @Test
    @DisplayName("env · configprops · beans · heapdump 는 관리 포트에서도 닫혀 있다")
    void dangerousEndpointsStayClosed() throws Exception {
        for (String path : new String[] {"env", "configprops", "beans", "heapdump"}) {
            assertThat(ActuatorProbe.get(managementPort, "/actuator/" + path).statusCode())
                    .as("/actuator/%s 가 열려 있다", path)
                    .isEqualTo(404);
        }
    }

    /**
     * 포트를 나눈 이유가 이것입니다. 업무 포트에서도 지표가 나오면 문이 하나뿐인 것과 같고,
     * Compose 가 업무 포트만 호스트로 내보내는 순간 지표가 인증 없이 열립니다.
     */
    @Test
    @DisplayName("업무 포트에는 actuator 가 붙지 않는다")
    void businessPortServesNoActuator() throws Exception {
        assertThat(serverPort)
                .as("두 포트가 같으면 아래 단언이 무슨 뜻인지 알 수 없다")
                .isNotEqualTo(managementPort);

        assertThat(ActuatorProbe.get(serverPort, "/actuator/prometheus").statusCode())
                .isEqualTo(404);
    }

}
