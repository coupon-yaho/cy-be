package com.kafkick.core.queuegateway;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.observation.QueueMode;

/**
 * 외부 대기열 게이트웨이가 읽는 상태를 기술 중립적으로 공급하는 경계입니다.
 *
 * <p>구현은 빈 인스턴스 ID, 음수 처리량, 0 이하 스냅샷 버전, 중복 회차처럼 계약을 위반한
 * 입력을 {@link IllegalArgumentException}으로 거부할 수 있습니다. 저장소 장애도 런타임 예외로
 * 전달되므로 호출자는 주기 작업과 업무 요청 경계를 분리해야 합니다.</p>
 */
public interface QueueGatewayStatePort {

    /** API 인스턴스가 안전하게 받을 수 있는 초당 처리량과 보고 시각을 기록합니다. */
    void reportCapacity(String instanceId, long creditsPerSecond, Instant reportedAt);

    /** 종료하는 API 인스턴스의 처리 가능량 필드를 제거합니다. */
    void removeCapacity(String instanceId);

    /** 여러 writer가 공유하는 단조 증가 스냅샷 버전을 예약합니다. */
    long reserveCouponRoundSnapshotVersion();

    /**
     * 활성 쿠폰 회차, 가용 재고 미러, 대기열 정책을 한 스냅샷으로 반영합니다.
     * 이미 적용된 버전 이하의 스냅샷은 현재 상태를 덮어쓰지 않습니다.
     */
    void publishCouponRounds(
            long snapshotVersion,
            List<QueueGatewayCouponRoundState> couponRounds,
            QueueMode queueMode
    );
}
