package com.kafkick.api.observation;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.LongStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.mockito.InOrder;

import com.kafkick.core.observation.CampaignLifecycleRecorder;
import com.kafkick.core.observation.ClosedCampaign;
import com.kafkick.core.observation.ClosedCampaignRecoverySource;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(OutputCaptureExtension.class)
class CampaignLifecycleStartupRecoveryTest {

    private static final Instant NOW =
            Instant.parse("2026-08-26T05:04:00Z");
    private static final Instant DAY_AGO =
            Instant.parse("2026-08-25T05:04:00Z");

    @Test
    @DisplayName("최근 1일 최신 1000건을 조회하고 오래된 종료부터 회수한다")
    void recoverOldestFirstAfterSelectingNewest() throws Exception {
        ClosedCampaignRecoverySource source =
                mock(ClosedCampaignRecoverySource.class);
        CampaignLifecycleRecorder recorder =
                mock(CampaignLifecycleRecorder.class);
        ClosedCampaign newest = new ClosedCampaign(203L,
                Instant.parse("2026-08-26T05:03:00Z"));
        ClosedCampaign sameTimeHigherId = new ClosedCampaign(202L,
                Instant.parse("2026-08-26T05:02:00Z"));
        ClosedCampaign sameTimeLowerId = new ClosedCampaign(201L,
                Instant.parse("2026-08-26T05:02:00Z"));
        when(source.findRecentlyClosed(DAY_AGO, NOW, 1_000))
                .thenReturn(List.of(
                        newest,
                        sameTimeHigherId,
                        sameTimeLowerId
                ));
        CampaignLifecycleStartupRecovery recovery = recovery(
                source,
                recorder
        );

        recovery.run(null);

        verify(source).findRecentlyClosed(DAY_AGO, NOW, 1_000);
        InOrder order = inOrder(recorder);
        order.verify(recorder).retireCampaign(
                sameTimeLowerId.campaignCouponId(),
                sameTimeLowerId.closedAt()
        );
        order.verify(recorder).retireCampaign(
                sameTimeHigherId.campaignCouponId(),
                sameTimeHigherId.closedAt()
        );
        order.verify(recorder).retireCampaign(
                newest.campaignCouponId(),
                newest.closedAt()
        );
    }

    @Test
    @DisplayName("DB 보정 실패는 API 기동을 막지 않고 미터를 건드리지 않는다")
    void isolateRecoverySourceFailure() {
        ClosedCampaignRecoverySource source =
                mock(ClosedCampaignRecoverySource.class);
        CampaignLifecycleRecorder recorder =
                mock(CampaignLifecycleRecorder.class);
        when(source.findRecentlyClosed(DAY_AGO, NOW, 1_000))
                .thenThrow(new IllegalStateException("db unavailable"));
        CampaignLifecycleStartupRecovery recovery = recovery(
                source,
                recorder
        );

        assertThatCode(() -> recovery.run(null))
                .doesNotThrowAnyException();
        verifyNoInteractions(recorder);
    }

    @Test
    @DisplayName("최근 종료 캠페인의 미터 회수 요청 건수를 기록한다")
    void logRetirementRequestCount(CapturedOutput output) throws Exception {
        ClosedCampaignRecoverySource source =
                mock(ClosedCampaignRecoverySource.class);
        CampaignLifecycleRecorder recorder =
                mock(CampaignLifecycleRecorder.class);
        when(source.findRecentlyClosed(DAY_AGO, NOW, 1_000))
                .thenReturn(List.of(new ClosedCampaign(201L, DAY_AGO)));

        recovery(source, recorder).run(null);

        assertThat(output)
                .contains("최근 종료 캠페인의 미터 회수를 요청했습니다.")
                .contains("count=1");
    }

    @Test
    @DisplayName("최근 종료 캠페인 조회가 상한에 도달하면 경고한다")
    void warnWhenRecoveryLimitIsReached(CapturedOutput output)
            throws Exception {
        ClosedCampaignRecoverySource source =
                mock(ClosedCampaignRecoverySource.class);
        CampaignLifecycleRecorder recorder =
                mock(CampaignLifecycleRecorder.class);
        List<ClosedCampaign> campaigns = LongStream.rangeClosed(1, 1_000)
                .mapToObj(id -> new ClosedCampaign(id, DAY_AGO))
                .toList();
        when(source.findRecentlyClosed(DAY_AGO, NOW, 1_000))
                .thenReturn(campaigns);

        recovery(source, recorder).run(null);

        assertThat(output)
                .contains("최근 종료 캠페인 조회가 상한에 도달했습니다.")
                .contains("일부 캠페인이 미터 회수 대상에서 제외되었을 수 있습니다.")
                .contains("count=1000")
                .contains("limit=1000");
    }

    private static CampaignLifecycleStartupRecovery recovery(
            ClosedCampaignRecoverySource source,
            CampaignLifecycleRecorder recorder
    ) {
        return new CampaignLifecycleStartupRecovery(
                source,
                recorder,
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC))
        );
    }
}
