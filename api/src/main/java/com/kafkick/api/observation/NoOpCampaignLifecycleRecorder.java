package com.kafkick.api.observation;

import com.kafkick.core.observation.CampaignLifecycleRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * 캠페인 수명 통지를 버리는 기본 구현입니다.
 *
 * <p>값 계약 위반도 호출자에게 던지지 않고 WARN 으로만 남깁니다. 종료 통지는 캠페인 종료
 * 트랜잭션 안에서 호출될 수 있어, 여기서 던지면 관측이 업무를 되돌립니다. 그렇다고 통과시키면
 * 잘못된 값이 실구현(OBS-26)이 붙는 날까지 조용히 흘러 다닙니다. 로그는 그 둘 사이입니다.
 *
 * <p>로그를 아무도 보지 않으면 이 방어는 없는 것과 같습니다. 위반 <b>횟수</b>를 세는 지표는
 * 아직 없습니다 — 이 티켓은 미터를 만들지 않습니다.
 * TODO(OBS-25): 계약 위반 Counter 를 붙인다.
 */
public final class NoOpCampaignLifecycleRecorder implements CampaignLifecycleRecorder {

    private static final Logger log =
            LoggerFactory.getLogger(NoOpCampaignLifecycleRecorder.class);

    @Override
    public void retireCampaign(long campaignCouponId, Instant closedAt) {
        if (campaignCouponId <= 0 || closedAt == null) {
            log.warn(
                    "캠페인 수명 통지의 값 계약을 위반했습니다. 통지를 버리고 계속 진행합니다. "
                            + "campaignCouponId={}, closedAt={}",
                    campaignCouponId,
                    closedAt
            );
            return;
        }
        // 수명 관리 구현이 없어도 호출부를 먼저 붙일 수 있어야 한다.
    }
}
