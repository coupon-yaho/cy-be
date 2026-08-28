package com.kafkick.api.observation.issuance;

import java.util.Objects;
import java.util.Optional;

import com.kafkick.api.observation.ObservationIssuanceProperties;
import com.kafkick.core.member.Grade;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import com.kafkick.core.support.TimeProvider;

public final class IssuanceObservationContextFactory {

    private final RuntimeConfigStore runtimeConfigStore;
    private final TimeProvider timeProvider;
    private final String producerInstanceId;

    public IssuanceObservationContextFactory(
            RuntimeConfigStore runtimeConfigStore,
            TimeProvider timeProvider,
            ObservationIssuanceProperties properties
    ) {
        this.runtimeConfigStore = Objects.requireNonNull(
                runtimeConfigStore
        );
        this.timeProvider = Objects.requireNonNull(timeProvider);
        this.producerInstanceId = Objects.requireNonNull(
                properties,
                "properties"
        ).producerInstanceId();
    }

    /**
     * 필터가 확정한 요청 ID, 실제 라우팅 엔진과 Runtime 설정 한 번의 조회로 발급 관측 Context를 만듭니다.
     *
     * <p>{@link RuntimeConfigStore#get()} 구현이 이미 last-known-good를 반영하므로 별도 재조회하지
     * 않습니다. 값이 없는 상태는 잘못된 기본값으로 이벤트를 만들지 않고 빈 결과로 반환합니다.
     *
     * @param requestId 요청 필터가 확정한 최대 36자 식별자
     * @param memberId 회원 식별자
     * @param couponId 쿠폰 회차 식별자
     * @param membershipGrade 요청 시점 회원 등급
     * @param engineVersion 회차 라우터가 이 요청에 확정한 실제 발급 엔진
     * @return 이벤트에 필요한 설정 값이 있으면 Context, 없으면 빈 결과
     */
    public Optional<IssuanceFlowEvent.Ctx> create(
            String requestId,
            long memberId,
            long couponId,
            MembershipGrade membershipGrade,
            EngineVersion engineVersion
    ) {
        RuntimeConfigSnapshot snapshot = runtimeConfigStore.get();
        if (!snapshot.status().carriesValue()) {
            return Optional.empty();
        }
        return Optional.of(new IssuanceFlowEvent.Ctx(
                requestId,
                memberId,
                couponId,
                grade(membershipGrade),
                false,
                timeProvider.instant(),
                Objects.requireNonNull(engineVersion, "engineVersion"),
                snapshot.releaseStage(),
                snapshot.queueMode(),
                null,
                producerInstanceId
        ));
    }

    /** 네 MembershipGrade 값을 이벤트 Grade로 빠짐없이 명시 변환합니다. */
    private static Grade grade(MembershipGrade membershipGrade) {
        return switch (Objects.requireNonNull(
                membershipGrade,
                "membershipGrade"
        )) {
            case WELCOME -> Grade.WELCOME;
            case SILVER -> Grade.SILVER;
            case GOLD -> Grade.GOLD;
            case VIP -> Grade.VIP;
        };
    }
}
