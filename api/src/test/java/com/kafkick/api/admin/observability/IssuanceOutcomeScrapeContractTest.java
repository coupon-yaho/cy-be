package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import com.kafkick.api.observation.issuance.CampaignMeterProperties;
import com.kafkick.api.observation.issuance.CampaignMeterRegistry;
import com.kafkick.core.observation.ReasonCode;

/**
 * 실패 원인 질의가 기대하는 <b>이름과 라벨이 실제 scrape 출력에 있는지</b> 봅니다.
 *
 * <p>이름이 어긋나도 예외는 안 납니다 — 질의가 빈 결과를 돌려주고 화면의 원인 표만 영원히
 * 비어 있을 뿐입니다. 조립기 테스트는 표본을 정한 이름으로 직접 만들어 넣으므로 이 어긋남을
 * 영원히 못 잡습니다. 그래서 실제 Prometheus 출력 문자열을 긁어 대조합니다.</p>
 */
class IssuanceOutcomeScrapeContractTest {

    /**
     * {@code CampaignMeterRegistry} 는 기동 시점에 사유 코드별 Counter 를 전부 등록하므로
     * 요청이 하나도 없어도 이름과 라벨은 나옵니다.
     */
    @Test
    @DisplayName("실패 사유 Counter 가 질의가 기대하는 이름·라벨로 scrape 에 나온다")
    void scrapeCarriesTheOutcomeNameAndLabelsTheQueryExpects() {
        PrometheusMeterRegistry meters = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try (CampaignMeterRegistry campaigns = new CampaignMeterRegistry(meters,
                new CampaignMeterProperties(8, Duration.ofMillis(1), Duration.ofHours(1), 10),
                Duration.ofSeconds(10))) {
            assertThat(campaigns).isNotNull();
            String scrape = meters.scrape();

            assertThat(scrape)
                    .as("이름이 어긋나면 예외 없이 원인 표만 빈다")
                    .contains(MetricAggregation.ISSUANCE_OUTCOME_TOTAL);
            for (ReasonCode reasonCode : OverviewPrometheusContract.FAILURE_REASONS) {
                assertThat(scrape)
                        .as("셀렉터가 고르는 라벨이 실제로 붙어 나와야 한다: " + reasonCode)
                        .contains(OverviewPrometheusContract.OUTCOME + "=\"" + reasonCode.name() + "\"");
            }
        }
    }
}
