package com.kafkick.core.coupon.v2;

import java.util.Objects;
import java.util.function.Function;

import org.springframework.stereotype.Component;

import com.kafkick.core.observation.EngineVersion;

/** 회차 정의를 한 번 읽고 v1/v2 중 정확히 한 실행기만 호출한다. */
@Component
public final class CouponIssuanceRouter {

    private final CouponRoundIssuanceDefinitionCache definitions;

    public CouponIssuanceRouter(CouponRoundIssuanceDefinitionCache definitions) {
        this.definitions = Objects.requireNonNull(definitions, "definitions");
    }

    public <R> R route(
            long couponRoundId,
            Function<CouponRoundIssuanceDefinition, R> v1,
            Function<CouponRoundIssuanceDefinition, R> v2
    ) {
        CouponRoundIssuanceDefinition definition = definitions.get(couponRoundId);
        return definition.engineVersion() == EngineVersion.V2
                ? Objects.requireNonNull(v2, "v2").apply(definition)
                : Objects.requireNonNull(v1, "v1").apply(definition);
    }
}
