package com.kafkick.core.coupon.v2.port;

import java.util.Optional;

import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.observation.EngineVersion;

public interface CouponRoundIssuanceDefinitionRepository {

    Optional<CouponRoundIssuanceDefinition> findById(long couponRoundId);

    Optional<CouponRoundIssuanceDefinition> lockAndFindById(long couponRoundId);

    /**
     * 오픈 전 또는 마감 후 회차만 변경한다. 오픈 중이면 {@code false}.
     *
     * <p>이미 정의를 읽어 간 회차({@code issuance_engine_locked})도 {@code false} 다 —
     * 인스턴스가 캐시한 엔진과 DB 가 갈리면 한 회차가 v1·v2 로 동시에 돌기 때문이다.
     *
     * @throws IllegalArgumentException {@code engineVersion} 이 {@code null} 이거나 V3 일 때
     */
    boolean updateEngineVersionWhenNotOpen(
            long couponRoundId,
            EngineVersion engineVersion
    );
}
