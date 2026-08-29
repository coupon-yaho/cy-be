package com.kafkick.core.coupon.v2.query;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 기존 V1 발급 가능 목록과 분리된 V2 정의 조회 서비스다. */
@Service
public class CouponDefinitionQueryService {

    private final CouponDefinitionQueryPort queryPort;

    public CouponDefinitionQueryService(CouponDefinitionQueryPort queryPort) {
        this.queryPort = Objects.requireNonNull(queryPort);
    }

    @Transactional(readOnly = true)
    public List<CouponDefinition> findCandidates(Instant asOf) {
        return queryPort.findCandidates(Objects.requireNonNull(asOf));
    }
}
