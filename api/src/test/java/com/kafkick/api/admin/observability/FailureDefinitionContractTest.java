package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.support.TimeProvider;

/**
 * 실패 <b>비율</b>의 분자 정의가 스냅샷({@code /metrics})과 추세선({@code /metrics/series})에서
 * 같은지 고정합니다.
 *
 * <p><b>각각을 따로 보는 테스트로는 이 계약을 지킬 수 없습니다.</b> 두 경로가 각자 옳게 계산해도
 * 정의가 갈리면 화면의 두 숫자가 서로 다른 것을 셉니다 — 깨지지 않고 조용히 어긋납니다.</p>
 */
class FailureDefinitionContractTest {

    private static final TimeProvider FIXED_TIME =
            new TimeProvider(Clock.fixed(Instant.parse("2026-08-21T00:00:00Z"), ZoneOffset.UTC));

    /**
     * 정책상 거절과 클라이언트 오류는 시스템이 정상 동작한 결과다. 분자에 들어가면 정책 거절이
     * 장애로 보이고, 운영자가 없는 장애를 쫓는다.
     */
    @Test
    @DisplayName("실패 분자는 의존성·애플리케이션 실패 둘뿐이다")
    void systemFailuresAreExactlyTheTwoServerSideClasses() {
        assertThat(ResultClass.systemFailures())
                .containsExactlyInAnyOrder(
                        ResultClass.DEPENDENCY_FAILURE, ResultClass.APPLICATION_FAILURE);
        assertThat(ResultClass.systemFailures())
                .allSatisfy(failure -> assertThat(failure.isSuccess()).isFalse());
        assertThat(ResultClass.systemFailures())
                .doesNotContain(ResultClass.POLICY_REJECT, ResultClass.CLIENT_INVALID);
    }

    /**
     * 시계열 질의가 그 정의를 그대로 싣는지 실제 PromQL 문자열에서 확인합니다. 어느 한쪽만
     * 고치면 여기가 red 가 됩니다.
     */
    @Test
    @DisplayName("시계열 실패율 질의는 ResultClass 가 정한 분류만 센다")
    void seriesFailureQueryCountsExactlyThoseClasses() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        new PromSeriesAssembler(source, FIXED_TIME, PrometheusSeriesProperties.defaults())
                .assemble(MetricsWindow.ONE_MINUTE);

        String failureQuery = source.issued().stream()
                .filter(promQl -> promQl.contains(" / "))
                .findFirst()
                .orElseThrow(() -> new AssertionError("실패율 질의가 나가지 않았습니다."));

        Set<ResultClass> failures = ResultClass.systemFailures();
        assertThat(failureQuery).contains(ResultClass.promLabelAlternation(failures));
        for (ResultClass notCounted : ResultClass.values()) {
            if (failures.contains(notCounted)) {
                continue;
            }
            // 분모 셀렉터에는 result 라벨이 없으므로 분자에만 이름이 실린다.
            assertThat(failureQuery)
                    .as("%s 는 실패 분자가 아닌데 질의에 실려 있습니다", notCounted)
                    .doesNotContain(notCounted.name().toLowerCase(java.util.Locale.ROOT));
        }
    }
}
