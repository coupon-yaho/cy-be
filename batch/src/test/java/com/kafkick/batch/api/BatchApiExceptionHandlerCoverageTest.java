// 예외 매핑이 모든 admin 컨트롤러를 덮는지 확인합니다. 사람이 아니라 기계가 지킵니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>{@code BatchApiExceptionHandler} 가 admin 컨트롤러를 하나도 안 빠뜨렸는지 본다.</b>
 *
 * <p>그 advice 는 {@code assignableTypes} 에 컨트롤러 이름을 <b>하나씩</b> 적는다. 패키지
 * 전체로 넓히지 않는 이유가 그 파일에 적혀 있다 — <i>"이름을 하나씩 적으면 새 컨트롤러를
 * 만든 사람이 이 파일을 열어 보게 된다."</i>
 *
 * <p><b>그 장치가 한 번 안 통했다.</b> CY-590 의 {@code VerifyReportController} 가 등록 없이
 * 들어왔고, 404 로 설계한 {@code RUN_NOT_FOUND} 가 <b>500 + 스프링 기본 본문</b>으로 나갔다.
 * 이 저장소의 규약이 <i>"응답은 항상 {@code ResponseEnvelope} 로 감싼다"</i> 인데 그 경로에서만
 * 깨졌고, <b>아무 테스트도 그것을 안 막았다.</b>
 *
 * <p>기대는 한 번 어긋나면 두 번 어긋난다. 그래서 여기서 <b>등식</b>으로 고정한다 —
 * 실제로 매핑된 {@code batch.api} 컨트롤러 집합과 advice 가 적은 집합이 같아야 한다.
 *
 * <p><b>부분집합이 아니라 등식인 것이 결정이다.</b> advice 쪽에만 있는 이름도 잡는다 —
 * 컨트롤러를 지우고 등록을 안 지우면 그 배열이 <i>"없는 클래스를 가리키는 목록"</i> 이 되고,
 * 다음 사람이 그것을 근거로 새 항목을 더한다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000",
        "server.port=0",
        "management.server.port=0"
})
@Import(MySqlContainerConfig.class)
class BatchApiExceptionHandlerCoverageTest {

    @Autowired
    private RequestMappingHandlerMapping mappings;

    @Test
    @DisplayName("예외 매핑이 admin 컨트롤러 전부를 덮는다 — 빠지면 그 컨트롤러만 500 을 낸다")
    void adviceCoversEveryAdminController() {
        // **equals 로 거르면 안 된다.** 하위 패키지(com.kafkick.batch.api.report 등)에
        // 컨트롤러가 생기면 양쪽으로 다 깨진다 — 등록을 안 하면 이 테스트가 못 보고,
        // 등록을 하면 mapped 에 없어서 "advice 에만 있다" 로 헛되이 실패한다.
        // 우리 코드 전부를 본다. 이 저장소의 컨트롤러는 예외 없이 이 봉투를 따른다.
        Set<Class<?>> mapped = mappings.getHandlerMethods().values().stream()
                .map(method -> method.getBeanType())
                .filter(type -> type.getPackageName().startsWith("com.kafkick."))
                .collect(Collectors.toSet());

        RestControllerAdvice advice = AnnotationUtils.findAnnotation(
                BatchApiExceptionHandler.class, RestControllerAdvice.class);
        assertThat(advice)
                .as("advice 애노테이션이 사라지면 모든 도메인 예외가 500 으로 나간다")
                .isNotNull();

        assertThat(Set.of(advice.assignableTypes()))
                .as("등록이 빠진 컨트롤러는 404 로 설계한 예외를 500 으로 낸다. "
                        + "그리고 응답 봉투(ResponseEnvelope)도 안 씌워져 클라이언트가 "
                        + "형식 두 벌을 만난다. 새 컨트롤러를 만들었으면 "
                        + "BatchApiExceptionHandler 의 assignableTypes 에 이름을 더해라")
                .containsExactlyInAnyOrderElementsOf(mapped);

        assertThat(mapped)
                .as("이 테스트가 뜻을 가지려면 컨트롤러가 실제로 매핑돼 있어야 한다")
                .isNotEmpty();
    }

    @Test
    @DisplayName("advice 가 적은 이름이 전부 실재하는 컨트롤러다 — 지운 뒤 남은 이름을 잡는다")
    void adviceListsNoStaleTypes() {
        RestControllerAdvice advice = AnnotationUtils.findAnnotation(
                BatchApiExceptionHandler.class, RestControllerAdvice.class);
        // findAnnotation 은 null 을 준다. 바로 쓰면 실패 메시지가 NPE 뿐이라,
        // 애노테이션이 사라진 것인지 목록이 틀린 것인지 안 드러난다.
        assertThat(advice)
                .as("advice 애노테이션이 사라지면 모든 도메인 예외가 500 으로 나간다")
                .isNotNull();

        assertThat(Arrays.stream(advice.assignableTypes())
                .filter(type -> !type.isAnnotationPresent(
                        org.springframework.web.bind.annotation.RestController.class))
                .toList())
                .as("컨트롤러가 아닌 것이 목록에 남아 있으면, 다음 사람이 그것을 근거로 "
                        + "새 항목을 더한다")
                .isEmpty();
    }
}
