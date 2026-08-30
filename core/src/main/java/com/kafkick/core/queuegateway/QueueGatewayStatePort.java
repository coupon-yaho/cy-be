package com.kafkick.core.queuegateway;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.observation.QueueMode;

/** 외부 대기열 게이트웨이가 읽는 Redis 상태를 기술 중립적으로 공급하는 경계입니다. */
public interface QueueGatewayStatePort {

    /** API 인스턴스가 안전하게 받을 수 있는 초당 처리량과 보고 시각을 기록합니다. */
    void reportCapacity(String instanceId, long creditsPerSecond, Instant reportedAt);

    /** 종료하는 API 인스턴스의 처리 가능량 필드를 제거합니다. */
    void removeCapacity(String instanceId);

    /** 활성 쿠폰 회차, 가용 재고 미러, 대기열 정책을 한 스냅샷으로 반영합니다. */
    void publishCouponRounds(List<QueueGatewayCouponRoundState> couponRounds, QueueMode queueMode);
}
