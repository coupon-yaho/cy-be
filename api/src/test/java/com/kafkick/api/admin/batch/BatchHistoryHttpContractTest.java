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

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>실제 톰캣 위에서</b> 경계와 인증이 걸리는지 본다.
 *
 * <p>{@code BatchHistoryControllerTest} 는 {@code MockMvcBuilders.standaloneSetup} 이라
 * <b>검증기와 인터셉터를 테스트가 손으로 조립한다.</b> 그 조립이 실제 기동 경로와 어긋나면
 * 거기서는 400 이 나는데 운영에서는 안 나는 상태가 되고, 아무 신호가 없다 —
 * {@code limit=100000} 이 그대로 SQL 에 실려 관측 풀의 {@code max_execution_time} 에 잘린다.
 *
 * <p>특히 이 컨트롤러는 파라미터 제약을 {@code @Min}/{@code @Max} 로만 걸고
 * <b>클래스에 {@code @Validated} 가 없다.</b> Spring Framework 6.1+ 가 컨트롤러 메서드
 * 파라미터의 제약을 자동으로 검증해 주기 때문인데, <b>그것은 프레임워크 버전에 딸린 동작</b>이다.
 * 추측하지 않고 실제 HTTP 로 확인한다.
 *
 * <p>인증도 같은 자리에서 본다. {@code Caller} 파라미터는 값을 쓰지 않지만
 * {@code X-User-Id} 를 <b>필수로 만드는</b> 역할을 하고, 관리자 역할 확인은 그 밖에서
 * {@code AdminAuthorizationInterceptor} 가 한다 — 두 장치가 함께 걸려야 이 경로가 닫힌다.
 */
@SpringBootTest(classes = com.kafkick.ApiApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(MySqlContainerConfig.class)
class BatchHistoryHttpContractTest {

    private static final String PATH = "/api/v1/admin/batch-executions";

    @Value("${local.server.port}")
    int port;

    private HttpResponse<String> call(String query, String userId, String role) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + PATH + query));
        if (userId != null) {
            builder.header("X-User-Id", userId);
        }
        if (role != null) {
            builder.header("X-User-Role", role);
        }
        return HttpClient.newHttpClient()
                .send(builder.GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @DisplayName("limit 경계가 실제 톰캣에서도 400 을 낸다")
    void limitBoundsAreEnforcedOverRealHttp() throws Exception {
        assertThat(call("?limit=201", "812934", "ADMIN").statusCode())
                .as("@Validated 없이 파라미터 제약이 걸리는지는 프레임워크 버전에 딸린 동작이다")
                .isEqualTo(400);
        assertThat(call("?limit=0", "812934", "ADMIN").statusCode()).isEqualTo(400);
    }

    @Test
    @DisplayName("경계 안이면 200 과 원천 표시를 돌려준다")
    void inRangeRequestSucceeds() throws Exception {
        HttpResponse<String> response = call("?limit=50", "812934", "ADMIN");

        // 거부가 항상 참이면 위 테스트는 아무것도 검증하지 않는다.
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"source\":\"BATCH_JOB_EXECUTION\"");
    }

    @Test
    @DisplayName("X-User-Id 가 없으면 400 이다")
    void missingCallerHeaderIsRejected() throws Exception {
        // Caller 를 Optional 로 선언하면 이 방어가 조용히 사라진다.
        assertThat(call("?limit=50", null, "ADMIN").statusCode()).isEqualTo(400);
    }

    @Test
    @DisplayName("관리자 역할이 없으면 403 이다")
    void missingAdminRoleIsForbidden() throws Exception {
        assertThat(call("?limit=50", "812934", null).statusCode()).isEqualTo(403);
        assertThat(call("?limit=50", null, null).statusCode()).isEqualTo(403);
    }
}
