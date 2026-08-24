package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorClassKey;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TrafficKey;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TrafficMetrics;
import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.core.observation.ReasonCode;

/**
 * {@code errors} 블록의 키가 <b>다른 파일에 있는 정본과 계속 붙어 있는지</b> 봅니다.
 *
 * <p>여기서 보는 것은 모두 두 파일에 걸친 계약입니다 — 분류 키는 {@link ResultClass} 와,
 * 분모 키는 {@link TrafficMetrics} 와, 원인 셀렉터는 {@link ReasonCode} 와 붙어 있어야 합니다.
 * 각각을 따로 검증하면 한쪽만 바뀐 것을 아무도 못 잡습니다 — 예외도 안 나고 화면의 행 하나가
 * 조용히 사라질 뿐입니다.</p>
 */
class ErrorPanelContractTest {

    /**
     * 실패 분류는 원천의 <b>성공이 아닌 네 분류</b>와 일대일입니다. 원천에 없는 것을 그리거나
     * 원천에 있는 것을 빠뜨리면 화면의 실패 표가 트래픽의 일부를 설명하지 못합니다.
     */
    @Test
    @DisplayName("실패 분류 키는 ResultClass 의 비성공 분류와 정확히 대응한다")
    void errorClassKeysMirrorNonSuccessResultClasses() {
        List<String> resultClasses = Arrays.stream(ResultClass.values())
                .filter(resultClass -> !resultClass.isSuccess())
                .map(Enum::name)
                .toList();

        assertThat(Arrays.stream(ErrorClassKey.values()).map(Enum::name).toList())
                .as("원천 분류가 늘거나 줄면 여기서 잡혀야 한다")
                // 표에 찍히는 순서는 화면의 선택이라 순서까지 묶지 않는다.
                .containsExactlyInAnyOrderElementsOf(resultClasses);
    }

    /** 화면은 서버가 준 키로 분모 이름을 찾습니다. 이름이 없으면 캡션이 키 문자열을 그대로 그립니다. */
    @Test
    @DisplayName("분모 키는 traffic 블록의 항목 이름과 같다")
    void trafficKeysMirrorTrafficMetricsComponents() {
        List<String> components = Arrays.stream(TrafficMetrics.class.getRecordComponents())
                .map(RecordComponent::getName)
                .toList();

        assertThat(Arrays.stream(TrafficKey.values()).map(TrafficKey::jsonValue).toList())
                .containsExactlyElementsOf(components);
    }

    /** 키는 화면 계약의 camelCase 그대로 나가야 합니다. 대문자 상수 이름이 나가면 화면이 못 읽습니다. */
    @Test
    @DisplayName("키는 camelCase 로 직렬화된다")
    void keysSerializeAsCamelCase() {
        assertThat(ErrorClassKey.DEPENDENCY_FAILURE.jsonValue()).isEqualTo("dependencyFailure");
        assertThat(ErrorClassKey.APPLICATION_FAILURE.jsonValue()).isEqualTo("applicationFailure");
        assertThat(ErrorClassKey.CLIENT_INVALID.jsonValue()).isEqualTo("clientInvalid");
        assertThat(ErrorClassKey.POLICY_REJECT.jsonValue()).isEqualTo("policyReject");
        assertThat(TrafficKey.ISSUE_ATTEMPT_RPS.jsonValue()).isEqualTo("issueAttemptRps");
    }

    /**
     * 원인 표의 셀렉터와 응답을 접는 쪽이 같은 목록을 봐야 합니다. 여기서는 그 목록이
     * <b>정책 거절을 담지 않는지</b>를 봅니다 — 담기면 재고 소진 구간에서 실제 장애 원인이
     * 상위 행 밖으로 밀립니다.
     */
    @Test
    @DisplayName("실패 사유 목록은 서버 실패만 담고 셀렉터도 그 목록에서 나온다")
    void failureReasonsCarryServerFailuresOnly() {
        assertThat(OverviewPrometheusContract.FAILURE_REASONS)
                .containsExactly(ReasonCode.TEMPORARILY_UNAVAILABLE, ReasonCode.INTERNAL_ERROR,
                        ReasonCode.UNMAPPED);

        String query = OverviewPrometheusContract.failureReasonRates(Duration.ofMinutes(1));
        for (ReasonCode reasonCode : ReasonCode.values()) {
            assertThat(query.contains(reasonCode.name()))
                    .as("셀렉터와 목록이 어긋나면 예외 없이 행 하나가 사라진다: " + reasonCode)
                    .isEqualTo(OverviewPrometheusContract.FAILURE_REASONS.contains(reasonCode));
        }
        // 재고 소진 사유가 원인 표에 섞이면 실패 원인 Top N 이 정책 표가 된다.
        assertThat(query).doesNotContain(ReasonCode.STOCK_EXHAUSTED.name());
    }

    /**
     * 이 목록은 <b>전역 상태</b>입니다 — 셀렉터를 만드는 쪽과 응답을 접는 쪽이 같은 것을 봅니다.
     * 가변으로 두면 누구든 한 줄로 관제 전체의 실패 사유 집합을 바꿀 수 있고, 그 변경은
     * 재기동 전까지 모든 요청에 남습니다.
     */
    @Test
    @DisplayName("실패 사유 목록은 밖에서 바꿀 수 없다")
    void failureReasonsCannotBeMutatedFromOutside() {
        assertThatThrownBy(() -> OverviewPrometheusContract.FAILURE_REASONS.add(ReasonCode.STOCK_EXHAUSTED))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> OverviewPrometheusContract.FAILURE_REASONS.clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * 순회 순서가 그대로 셀렉터 문자열이 됩니다. 순서가 없는 Set 으로 바꾸면 같은 질의가
     * 기동마다 다른 문자열이 되어 Prometheus 쪽 질의 캐시와 로그 대조가 안 맞습니다.
     */
    @Test
    @DisplayName("셀렉터 문자열은 기동마다 같다")
    void selectorStringIsStableAcrossCalls() {
        assertThat(OverviewPrometheusContract.failureReasonRates(Duration.ofMinutes(1)))
                .isEqualTo(OverviewPrometheusContract.failureReasonRates(Duration.ofMinutes(1)))
                .contains("TEMPORARILY_UNAVAILABLE|INTERNAL_ERROR|UNMAPPED");
    }
}
