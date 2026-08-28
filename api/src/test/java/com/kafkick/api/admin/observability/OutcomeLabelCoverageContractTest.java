package com.kafkick.api.admin.observability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.ReasonCode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 계측이 <b>내보내는</b> outcome 라벨과 O3 가 <b>읽을 줄 아는</b> 라벨이 같은 집합이어야 한다.
 *
 * <p>두 파일에 걸친 계약이다. {@code CampaignMeterRegistry} 는 기동 시 {@link ReasonCode} 전부에
 * 카운터를 <b>미리 등록</b>하므로, 그 코드가 한 번도 발생하지 않아도 시계열은 0 값으로 존재하고
 * Prometheus 가 그대로 긁어간다. O3 는 모르는 라벨을 하나라도 보면
 * {@code onlyKnownOutcomeLabels} 에서 전체를 {@code UNAVAILABLE} 로 버린다 — 새 코드를 아무도
 * 안 밟아도 <b>배포 즉시 고객 결과 패널이 통째로 사라진다.</b>
 *
 * <p>한쪽만 늘렸을 때 컴파일도 기존 테스트도 안 깨지는 조합이라 여기서 잡는다.
 */
class OutcomeLabelCoverageContractTest {

    /** {@code CampaignMeterRegistry} 가 등록하는 라벨. 이유 코드 전부 + 발급 + 대기열. */
    private static List<String> registeredOutcomeLabels() {
        List<String> labels = new ArrayList<>(
                Arrays.stream(ReasonCode.values()).map(Enum::name).toList());
        labels.add("ISSUED");
        labels.add("QUEUED");
        return labels;
    }

    @Test
    @DisplayName("계측이 내보내는 모든 outcome 라벨을 O3 가 읽을 줄 안다")
    void everyRegisteredOutcomeLabelIsKnownToTheOverviewSource() {
        assertThat(PromOverviewObservationSource.knownOutcomeLabels())
                .containsAll(registeredOutcomeLabels());
    }

    /**
     * 반대 방향도 본다. O3 만 아는 라벨이 있으면 재고 조회가 그 라벨의 부재로 영영
     * {@code PENDING} 이다 — {@code completeOutcomeInventory} 가 개수 일치를 요구한다.
     */
    @Test
    @DisplayName("O3 가 아는 라벨 중 아무도 안 내보내는 것이 없다")
    void noKnownLabelIsUnexported() {
        assertThat(PromOverviewObservationSource.knownOutcomeLabels())
                .isSubsetOf(registeredOutcomeLabels());
    }
}
