// 만료 잡이 파라미터와 청크 도중 변화를 어떻게 막는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.expiration.exception.ExpirationErrorCode;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>둘 다 "정상으로 끝나면 더 나쁜" 실패다.</b>
 *
 * <ul>
 *   <li><b>미래 {@code asOf}</b> — 기한이 남은 발급건이 전부 컷 안에 들어와 만료된다.
 *       {@code EXPIRED} 는 종단 상태라 <b>되돌릴 수 없다.</b></li>
 *   <li><b>청크 도중 재고 행이 생긴 경우</b> — 예전에는 이 방향을 <i>"스키마상 불가능"</i> 으로
 *       보고 언더플로와 뭉쳐 던져서, 운영자에게 <b>틀린 안내</b>가 나갔다.</li>
 * </ul>
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.expire.chunk-size=100"
})
@Import({MySqlContainerConfig.class, ExpireGuardTest.ShrinkStockRowCountConfig.class})
class ExpireGuardTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job expireJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed seed;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
        ShrinkStockRowCountConfig.off();
    }

    /**
     * <b>스케줄러는 이 값을 못 만든다 — 손으로 트리거할 때 열리는 자리다.</b>
     * 설정 파일이 <i>"꺼진 채 두었다가 나중에 돌리면 따라잡는다"</i> 는 운영 절차를 스스로
     * 권하고 있고, 그 "나중에 돌리는" 손이 {@code asOf} 를 직접 친다.
     */
    @Test
    @DisplayName("asOf 가 미래면 기한이 남은 건을 건드리기 전에 멈춘다")
    void refusesFutureAsOf() throws Exception {
        long alive = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.plusDays(30))
                .param("id", alive)
                .update();
        seed.overwriteStock(1);

        // 고정 상수에 더하면 그 절대 시각을 벽시계가 지나는 날 미래가 아니게 된다.
        // 그때 깨지는 원인이 코드가 아니라 달력이라, 단언을 느슨하게 고칠 위험이 크다.
        JobExecution execution = launch(LocalDateTime.now().plusYears(1));

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(JobFailures.errorCodesOf(execution))
                .contains(ExpirationErrorCode.EXPIRE_ASOF_IN_FUTURE.getCode());
        assertThat(statusOf(alive))
                .as("**한 건이라도 넘어갔으면 되돌릴 수 없다.** EXPIRED 는 종단 상태다")
                .isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(activeCount())
                .as("재고도 그대로여야 한다")
                .isEqualTo(1);
    }

    /**
     * <b>청크 도중에 재고 행이 생기면 어느 가드로 나가는지 못 박는다.</b>
     *
     * <p>만료 Step 은 READ COMMITTED 라 문장마다 스냅샷이 갱신된다. 그래서 {@code stockRowCount}
     * 와 {@code releaseStock} 이 <b>다른 행 집합</b>을 볼 수 있다는 지적이 있었고, 그 방향으로
     * {@code released > stockRows} 전용 코드를 만들 뻔했다.
     *
     * <p><b>그 분기는 도달할 수 없다.</b> 그 앞에 {@code stockRows != coupons} 가드가 있어서,
     * 재고 행이 하나 모자란 상태는 {@code STOCK_ROW_MISSING} 으로 <b>먼저</b> 나간다.
     * 이 저장소는 같은 실수 — 스키마상 못 일어나는 상황에 가드를 세우는 것 — 를 이미 한 번
     * 하고 지웠다. 여기서 그 사실을 테스트로 고정한다.
     *
     * <p>진짜 결함은 그쪽 <b>메시지</b>였다. <i>"다시 돌려도 그 행은 여전히 없습니다"</i> 는
     * 누가 그 행을 만들고 있는 중이면 거짓이고, 운영자는 방금 자기가 넣은 행을 다시 의심한다.
     */
    @Test
    @DisplayName("재고 행이 모자라면 STOCK_ROW_MISSING 으로 먼저 나간다 — 언더플로와 안 섞인다")
    void reportsMissingStockRowBeforeAnythingElse() throws Exception {
        long target = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.minusDays(1))
                .param("id", target)
                .update();
        seed.overwriteStock(1);

        ShrinkStockRowCountConfig.on();
        JobExecution execution = launch(AS_OF);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(JobFailures.errorCodesOf(execution))
                .as("재고 행 축과 수량 축은 사람이 볼 곳이 다르다. 섞이면 없는 행을 찾으러 간다")
                .contains(ExpirationErrorCode.STOCK_ROW_MISSING.getCode())
                .doesNotContain(ExpirationErrorCode.STOCK_UNDERFLOW.getCode());
        assertThat(JobFailures.messagesOf(execution))
                .as("**누가 그 행을 만들고 있는 중일 수 있다.** 단정하면 운영자가 방금 자기가 "
                        + "넣은 행을 다시 의심한다")
                .anyMatch(message -> message.contains("다음 주기가 알아서 지나갑니다"));
    }

    private JobExecution launch(LocalDateTime asOf) throws Exception {
        return jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", asOf)
                .toJobParameters());
    }

    private String statusOf(long id) {
        return jdbcClient.sql("SELECT status FROM issuances WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private int activeCount() {
        return jdbcClient.sql("SELECT active_count FROM coupon_stocks ORDER BY coupon_id LIMIT 1")
                .query(Integer.class)
                .single();
    }

    /** {@code stockRowCount} 만 한 건 적게 돌려준다. 나머지는 실제 저장소 그대로다. */
    @TestConfiguration
    static class ShrinkStockRowCountConfig {

        private static volatile boolean shrink;

        static void on() {
            shrink = true;
        }

        static void off() {
            shrink = false;
        }

        @Bean
        static BeanPostProcessor shrinkStockRowCount() {
            return ExpirationProxies.decorating((real, method, args) -> {
                Object result = ExpirationProxies.callThrough(real, method, args);
                if (shrink && "stockRowCount".equals(method.getName())) {
                    return ((Integer) result) - 1;
                }
                return result;
            });
        }
    }
}
