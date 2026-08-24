package com.kafkick.batch.benchmark;

import java.util.List;

import org.springframework.core.env.Environment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;

import com.kafkick.batch.benchmark.TopologyValidator.Topology;
import com.kafkick.batch.benchmark.TopologyValidator.ValidationResult;
import com.kafkick.batch.benchmark.TopologyValidator.Violation;

/** API의 start gate가 조회하는 batch 로컬 preflight 경계. */
@RestController
@RequestMapping("/internal/v1/benchmarks")
public class TopologyPreflightController {

    private final TopologyValidator validator;
    private final Environment environment;

    @Autowired
    public TopologyPreflightController(Environment environment) {
        this(new TopologyValidator(environment.getProperty(
            "benchmark.topology.gap-interval-ms", Long.class, 30_000L)), environment);
    }

    TopologyPreflightController(TopologyValidator validator, Environment environment) {
        this.validator = validator;
        this.environment = environment;
    }

    @GetMapping("/preflight")
    public ResponseEntity<PreflightResponse> preflight(@RequestParam long couponId) {
        String gaugeCoupon = environment.getProperty("observation.domain-gauge.coupon-id");
        Topology topology = new Topology(
            environment.getProperty("batch.scheduling.enabled", Boolean.class, true),
            environment.getProperty("observation.domain-gauge.enabled", Boolean.class, false),
            environment.getProperty(
                "observation.domain-gauge.aggregate-interval-ms", Long.class, -1L),
            parseCouponId(gaugeCoupon),
            couponId);
        ValidationResult result = validator.validate(topology);
        PreflightResponse response = new PreflightResponse(result.valid(), result.violations());
        return ResponseEntity.status(result.valid() ? HttpStatus.OK : HttpStatus.CONFLICT).body(response);
    }

    private static Long parseCouponId(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public record PreflightResponse(boolean valid, List<Violation> violations) {
        public PreflightResponse {
            violations = List.copyOf(violations);
        }
    }
}
