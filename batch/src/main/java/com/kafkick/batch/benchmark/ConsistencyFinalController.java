package com.kafkick.batch.benchmark;

import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import com.kafkick.batch.benchmark.TopologyValidator.Violation;
import com.kafkick.batch.observation.ConsistencyRawValueReader;
import com.kafkick.batch.observation.ConsistencyRawValueReader.DomainRawSnapshot;
import com.kafkick.batch.observation.DomainGaugeProperties;
import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.ConsistencyErrorCode;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.ConsistencyRawSnapshot;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.exception.BusinessException;

/** LIVE와 같은 reader를 사용해 FINAL 정합성 결과를 계산하는 batch 내부 경계입니다. */
@RestController
@RequestMapping("/internal/v1/benchmarks")
@ConditionalOnProperty(name = {
        "observation.datasource.enabled", "observation.domain-gauge.enabled"
}, havingValue = "true")
public class ConsistencyFinalController {
    private final ConsistencyRawValueReader reader;
    private final ConsistencyCalculator calculator;
    private final DomainGaugeProperties properties;

    public ConsistencyFinalController(
            ConsistencyRawValueReader reader,
            ConsistencyCalculator calculator,
            DomainGaugeProperties properties) {
        this.reader = reader;
        this.calculator = calculator;
        this.properties = properties;
    }

    /**
     * 어긋난 회차 요청은 500이 아니라 원인을 담은 409로, 원천이 아직 유효하지 않은 경우는
     * 503으로 돌려준다. api가 이 본문을 consistency_failure_reason에 그대로 실어야
     * 재실행 판단이 가능하다.
     */
    @PostMapping("/consistency/final")
    public ResponseEntity<?> evaluate(
            @RequestParam long couponId,
            @RequestParam EngineVersion engineVersion) {
        List<Violation> violations = ConsistencyFinalGuard.checkEngineVersion(
                properties.engineVersion(), engineVersion);
        if (!violations.isEmpty()) {
            return conflict(violations);
        }
        DomainRawSnapshot snapshot;
        try {
            snapshot = reader.read();
        } catch (DataAccessException failure) {
            return unavailable("observation.consistency.source-read",
                    "readable", failure.getClass().getSimpleName(),
                    "관측 풀에서 원시값을 읽지 못했습니다: " + failure.getClass().getSimpleName());
        }
        violations = ConsistencyFinalGuard.checkCouponId(snapshot.couponId(), couponId);
        if (!violations.isEmpty()) {
            return conflict(violations);
        }
        try {
            return ResponseEntity.ok(calculator.evaluate(
                    snapshot.consistency(), ConsistencyPhase.FINAL, engineVersion));
        } catch (BusinessException failure) {
            // 어떤 도메인 오류든 원인이 본문에 실려야 api 가 재실행 판단 근거로 저장한다.
            // FINAL_VALUE_UNAVAILABLE 만 "지금은 못 한다"라 503, 나머지는 값 자체가 못 쓴다.
            String key = "observation.consistency."
                    + failure.getErrorCode().getCode().toLowerCase(java.util.Locale.ROOT);
            String states = sourceStates(snapshot.consistency());
            if (failure.getErrorCode() == ConsistencyErrorCode.FINAL_VALUE_UNAVAILABLE) {
                return unavailable(key, "VALID", states, failure.getMessage());
            }
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(new MismatchResponse(List.of(new Violation(
                            key, "VALID", states, failure.getMessage()))));
        }
    }

    private static ResponseEntity<MismatchResponse> unavailable(
            String key, String expected, String actual, String reason) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new MismatchResponse(List.of(new Violation(key, expected, actual, reason))));
    }

    private static ResponseEntity<MismatchResponse> conflict(List<Violation> violations) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new MismatchResponse(violations));
    }

    private static String sourceStates(ConsistencyRawSnapshot snapshot) {
        return "redis=" + snapshot.redisObservation().status()
                + ",db=" + snapshot.databaseObservation().status();
    }

    /** 회차 불일치·원천 미가용 원인을 api가 그대로 저장할 수 있게 만든 응답입니다. */
    public record MismatchResponse(List<Violation> violations) {
        public MismatchResponse {
            violations = List.copyOf(violations);
        }
    }
}
