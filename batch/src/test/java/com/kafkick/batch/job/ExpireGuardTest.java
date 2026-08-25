// 만료 잡이 파라미터와 청크 도중 변화를 어떻게 막는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;

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
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.FixedClock;
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
        // 되읽기가 같은 ExpirationRepository 프록시에 countPending 을 부른다 —
        // 백그라운드 틱이 호출 기록에 끼면 재현 불가로 빨개진다.
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.expire.chunk-size=100"
})
@Import({MySqlContainerConfig.class, ExpireGuardTest.FailStockLockConfig.class,
        ExpireGuardTest.LeakingExclusionConfig.class, ExpireGuardTest.FixedClockConfig.class})
class ExpireGuardTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /** 고정한 "지금". 가드가 {@code asOf} 를 이 값과 견준다. */
    private static final LocalDateTime NOW = AS_OF.plusMinutes(3);

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
        FailStockLockConfig.off();
        LeakingExclusionConfig.off();
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

        // 시계를 고정했으니 "미래" 가 실행 날짜와 무관하다. 경계 바로 밖을 찍는다 —
        // plusYears 같은 큰 값은 조건을 한 칸 옮겨도 통과한다.
        //
        // 가드는 CronSlot 의 조기 발화 관용 폭(2초)만큼 열려 있다. 그 안은 스케줄러가 실제로
        // 만들 수 있는 값이라 막으면 정상 주기가 죽는다. 여기서는 그 밖을 찍는다.
        JobExecution execution = launch(NOW.plusSeconds(3));

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
     * <b>경계는 열려 있어야 한다.</b> 스케줄러가 주는 {@code asOf} 는 <b>크론 슬롯 시각</b>이라,
     * 정시에 뜨면 {@code now} 와 같은 순간이 된다. 그것까지 막으면 정상 주기가 안 돈다.
     *
     * <p>위 테스트와 짝이다 — 막는 쪽만 보면 <b>항상 던지는 가드</b>도 통과한다.
     */
    @Test
    @DisplayName("asOf 가 정확히 현재면 통과한다 — 경계는 열려 있다")
    void acceptsAsOfEqualToNow() throws Exception {
        long alive = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", NOW.plusDays(30))
                .param("id", alive)
                .update();
        seed.overwriteStock(1);

        assertThat(launch(NOW).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(launch(NOW.plusSeconds(2)).getStatus())
                .as("조기 발화 관용 폭(CronSlot.EARLY_FIRE_TOLERANCE) 안은 스케줄러가 실제로 "
                        + "만드는 값이다. 막으면 시계가 조금 흔들린 주기가 통째로 죽는다")
                .isEqualTo(BatchStatus.COMPLETED);
    }

    /**
     * <b>재고 행이 없는 회차를 만났을 때 어느 가드로 나가는지 못 박는다.</b>
     *
     * <p><b>이 판정이 청크의 맨 앞으로 옮겨 왔다.</b> 예전에는 만료를 넘긴 <i>뒤에</i>
     * {@code expiredCouponCount} 와 {@code stockRowCount} 를 견줘 알았다 — 그 시점이면 이미
     * <i>"재고 없이 만료된 상태"</i> 가 트랜잭션 안에 만들어져 있었고, 되돌리는 것은 롤백이었다.
     * 이제 {@code lockStock} 이 <b>아무것도 쓰기 전에</b> 잡는다.
     *
     * <p>그러면서 조회 둘이 통째로 없어졌다. 그 둘이 있던 이유가 <i>"두 실패를 갈라 보려고"</i>
     * 였는데, 지금은 갈라지는 자리가 서로 다르다 — 재고 행 <b>없음</b>은 여기,
     * 재고 <b>모자람</b>은 {@code releaseStock} 의 갱신 행 수 0 이다.
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

        FailStockLockConfig.on();
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

    /**
     * <b>이 가드는 이제 데이터가 아니라 코드를 가리킨다.</b>
     *
     * <p>회차 격리가 들어오기 전에는 재고가 어긋난 회차를 <b>실제 데이터로</b> 심으면 여기
     * 도달했다. 지금은 {@code blockedCoupons} 가 그런 회차를 창 밖으로 미리 빼므로,
     * <b>데이터로는 이 분기에 갈 수 없다.</b> 그러면 <i>"도달할 수 없는 가드는 없는 가드와
     * 같다"</i> 는 이 저장소의 기준에 걸린다 — 그래서 도달 경로를 주입으로 만든다.
     *
     * <p><b>주입하는 것이 반환값이 아니라 제외 목록인 것이 핵심이다.</b> 가드의 새 뜻이
     * <i>"제외 논리가 틀렸거나 재고가 발밑에서 움직였다"</i> 이므로, 재현해야 하는 것도
     * 정확히 그것 — <b>제외가 새는 상황</b>이다. {@code releaseStock} 의 반환값을 인위적으로
     * 줄이면 숫자는 같지만 뜻이 다른 상황을 재게 된다.
     *
     * <p>누가 {@code blockedCoupons} 의 {@code active_count < pending} 을 {@code <=} 로
     * 바꾸거나 {@code EXPIRE_BATCH} 의 {@code NOT IN} 절을 지우면 실제로 이 모양이 된다.
     * 그때 <b>재고를 안 되돌린 {@code EXPIRED} 가 조용히 커밋되지 않고</b> 여기서 죽는다.
     */
    @Test
    @DisplayName("제외 논리가 회차를 놓치면 그 자리에서 죽는다 — STOCK_UNDERFLOW")
    void failsWhenExclusionLeaksACoupon() throws Exception {
        long first = expiringIssuance();
        long second = expiringIssuance();
        seed.overwriteStock(1);

        LeakingExclusionConfig.on();
        JobExecution execution = launch(AS_OF);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(JobFailures.errorCodesOf(execution))
                .as("재고 행은 있다. 모자란 것은 수량이라 사람이 볼 곳이 다르다")
                .contains(ExpirationErrorCode.STOCK_UNDERFLOW.getCode())
                .doesNotContain(ExpirationErrorCode.STOCK_ROW_MISSING.getCode());
        assertThat(statusOf(first))
                .as("청크가 통째로 되돌아간다 — 재고를 안 되돌린 EXPIRED 는 커밋되면 안 된다")
                .isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(statusOf(second)).isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(activeCount())
                .as("재고도 그대로다")
                .isEqualTo(1);
    }

    private long expiringIssuance() {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.minusDays(1))
                .param("id", id)
                .update();
        return id;
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

    /**
     * <b>시계를 고정한다.</b> 가드는 {@code asOf.isAfter(timeProvider.now())} 로 판정하므로,
     * 시계가 벽시계면 이 테스트의 "미래" 가 실행하는 날짜에 달린다. 고정하면 판정에 들어가는
     * 두 값이 모두 테스트가 정한 값이 된다.
     *
     * <p>운영 {@code TimeConfig} 가 {@code systemUTC} 다 — 기본 타임존을 쓰면 CI 와 로컬이
     * 다른 값을 내고 고정한 의미가 없어진다.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class FixedClockConfig {

        @Bean
        @Primary
        Clock fixedClock() {
            return FixedClock.at(NOW);
        }
    }

    /**
     * {@code blockedCoupons} 만 <b>빈 목록</b>으로 덮는다. 제외 논리가 샌 상태를 재현한다 —
     * 어긋난 회차가 창 안으로 그대로 들어온다.
     */
    @TestConfiguration
    static class LeakingExclusionConfig {

        private static volatile boolean leak;

        static void on() {
            leak = true;
        }

        static void off() {
            leak = false;
        }

        @Bean
        static BeanPostProcessor leakExclusion() {
            return ExpirationProxies.decorating((real, method, args) -> {
                if (leak && "blockedCoupons".equals(method.getName())) {
                    return List.of();
                }
                return ExpirationProxies.callThrough(real, method, args);
            });
        }
    }

    /**
     * {@code lockStock} 만 <b>못 잠갔다</b>고 돌려준다. 나머지는 실제 저장소 그대로다.
     *
     * <p>실제로 재고 행을 지우면 {@code blockedCoupons} 가 그 회차를 창 밖으로 빼서 <b>이 분기에
     * 도달하지 못한다.</b> 여기서 재는 것은 <i>"재고 행이 없는 회차를 만들 수 있는가"</i> 가
     * 아니라 <i>"그 상태를 만났을 때 어느 가드로, 어떤 말로 나가는가"</i> 다.
     */
    @TestConfiguration
    static class FailStockLockConfig {

        private static volatile boolean fail;

        static void on() {
            fail = true;
        }

        static void off() {
            fail = false;
        }

        @Bean
        static BeanPostProcessor failStockLock() {
            return ExpirationProxies.decorating((real, method, args) -> {
                if (fail && "lockStock".equals(method.getName())) {
                    return false;
                }
                return ExpirationProxies.callThrough(real, method, args);
            });
        }
    }
}
