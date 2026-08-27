package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.core.expiration.exception.ExpirationErrorCode;
import com.kafkick.core.support.exception.BusinessException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.jdbc.UncategorizedSQLException;

/** {@link ExpireFailureMetrics} — 만료 실패를 에러코드로 가르는 축(docs/13 §2d). */
class ExpireFailureMetricsTest {

    private MeterRegistry registry;
    private ExpireFailureMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new ExpireFailureMetrics(registry);
    }

    private double count(String errorCode) {
        Counter counter = registry.find("cy_expire_failures_total")
                .tag("error_code", errorCode)
                .counter();
        return counter == null ? -1 : counter.count();
    }

    private static JobExecution execution() {
        return new JobExecution(1L, new JobInstance(1L, "expireJob"), new JobParameters());
    }

    private JobExecution failedWith(Throwable... causes) {
        JobExecution execution = execution();
        execution.setStatus(BatchStatus.FAILED);
        for (Throwable cause : causes) {
            execution.addFailureException(cause);
        }
        return execution;
    }

    @Test
    @DisplayName("실패 전에도 잡 실패 다섯이 0 으로 노출된다 — 첫 실패를 increase() 가 놓치지 않는다")
    void preRegistersJobFailureCodes() {
        for (ExpirationErrorCode code : ExpirationErrorCode.values()) {
            if (code.isJobFailure()) {
                assertThat(count(code.getCode()))
                        .as("%s 가 미리 등록돼 있어야 한다", code)
                        .isZero();
            }
        }
        assertThat(count(ExpireFailureMetrics.UNCLASSIFIED)).isZero();
    }

    /**
     * <b>목록을 손으로 적는다.</b> {@code values().length} 로 세면 006·007 이 다시 들어와도
     * 개수가 맞아떨어져 안 잡힌다 — 실제로 그렇게 새 코드를 통째로 등록했었다.
     */
    @Test
    @DisplayName("006·007 은 라벨에 없다 — 그 둘은 컨트롤러 거절 사유지 잡 실패가 아니다")
    void excludesControllerCodes() {
        assertThat(registry.find("cy_expire_failures_total").counters())
                .extracting(c -> c.getId().getTag("error_code"))
                .containsExactlyInAnyOrder(
                        "EXPIRATION-001", "EXPIRATION-002", "EXPIRATION-003",
                        "EXPIRATION-004", "EXPIRATION-005",
                        ExpireFailureMetrics.UNCLASSIFIED);

        assertThat(count(ExpirationErrorCode.EXPIRE_EXECUTION_NOT_FOUND.getCode()))
                .as("006 은 HTTP 404 — 만료와 무관한 라벨이 대시보드에 영원히 0 으로 남는다")
                .isEqualTo(-1);
        assertThat(count(ExpirationErrorCode.EXPIRE_EXECUTION_NOT_STUCK.getCode()))
                .as("007 은 HTTP 409")
                .isEqualTo(-1);
    }

    @Test
    @DisplayName("BusinessException 이면 그 에러코드로 센다")
    void countsByErrorCode() {
        metrics.afterJob(failedWith(
                new BusinessException(ExpirationErrorCode.STOCK_UNDERFLOW, "회차=7 만료=3")));

        assertThat(count(ExpirationErrorCode.STOCK_UNDERFLOW.getCode())).isEqualTo(1);
        assertThat(count(ExpireFailureMetrics.UNCLASSIFIED)).isZero();
    }

    @Test
    @DisplayName("감싸인 BusinessException 도 원인 사슬에서 찾아낸다")
    void unwrapsCause() {
        metrics.afterJob(failedWith(new UncategorizedSQLException(
                "expire", "update", new SQLException("boom") {
                    @Override
                    public synchronized Throwable getCause() {
                        return new BusinessException(ExpirationErrorCode.STOCK_ROW_MISSING);
                    }
                })));

        assertThat(count(ExpirationErrorCode.STOCK_ROW_MISSING.getCode()))
                .as("맨 위만 보면 우리가 낸 코드가 전부 UNCLASSIFIED 로 샌다")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("BusinessException 이 아니면 UNCLASSIFIED — 0 이 아니면 그 자체가 신호다")
    void countsUnclassified() {
        metrics.afterJob(failedWith(new IllegalStateException("안 세던 자리")));

        assertThat(count(ExpireFailureMetrics.UNCLASSIFIED)).isEqualTo(1);
    }

    @Test
    @DisplayName("한 실행이 여러 자리에서 죽으면 각각 센다")
    void countsEveryFailure() {
        metrics.afterJob(failedWith(
                new BusinessException(ExpirationErrorCode.STOCK_UNDERFLOW),
                new BusinessException(ExpirationErrorCode.EXPIRE_ASOF_IN_FUTURE)));

        assertThat(count(ExpirationErrorCode.STOCK_UNDERFLOW.getCode())).isEqualTo(1);
        assertThat(count(ExpirationErrorCode.EXPIRE_ASOF_IN_FUTURE.getCode())).isEqualTo(1);
    }

    @Nested
    @DisplayName("세지 않는 경우")
    class DoesNotCount {

        @Test
        @DisplayName("성공한 실행은 예외가 남아 있어도 안 센다 — 재시도가 삼킨 것이다")
        void ignoresSuccessfulRun() {
            JobExecution execution = execution();
            execution.setStatus(BatchStatus.COMPLETED);
            execution.addFailureException(
                    new BusinessException(ExpirationErrorCode.STOCK_UNDERFLOW));

            metrics.afterJob(execution);

            assertThat(count(ExpirationErrorCode.STOCK_UNDERFLOW.getCode())).isZero();
        }

        @Test
        @DisplayName("실패했는데 예외가 없으면 아무것도 안 센다")
        void handlesEmptyFailures() {
            metrics.afterJob(failedWith());

            assertThat(registry.find("cy_expire_failures_total").counters())
                    .allSatisfy(c -> assertThat(c.count()).isZero());
        }
    }

    @Test
    @DisplayName("사슬이 자기를 참조해도 안 멈춘다 — 깊이를 막는다")
    void survivesSelfReferencingCause() {
        Throwable loop = new IllegalStateException("고리") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };

        metrics.afterJob(failedWith(loop));

        assertThat(count(ExpireFailureMetrics.UNCLASSIFIED)).isEqualTo(1);
    }
}
