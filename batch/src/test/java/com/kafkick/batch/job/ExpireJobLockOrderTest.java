// 만료 잡이 테이블을 어떤 순서로 잠그는지 고정합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>락 순서는 혼자 지킬 수 없는 계약인데, 이 저장소에는 한쪽 발밖에 없다.</b>
 * 발급·취소 경로는 {@code feature/CY-5} 에 있고 여기서 컴파일되지 않는다. 그래서 이 테스트가
 * 지킬 수 있는 것은 <b>만료 쪽 순서뿐</b>이고, 상대가 바뀌면 이쪽 CI 는 아무 말도 안 한다.
 * <b>여기 단언을 고치기 전에 {@code docs/12-expire-lock-measurement.md} §11 을 읽어라.</b>
 *
 * <p><b>순서는 우리가 정하는 것이 아니다 — {@code coupon_stocks} → {@code issuances}
 * → {@code issuance_histories} 다.</b> 재고를 건드리는 사용자 경로가 이미 그 행을
 * {@code FOR UPDATE} 로 먼저 잡는다: 발급·취소는 항상, 사용취소는 {@code EXPIRED} 로
 * 갈 때만(보통 경로는 재고를 안 건드려 이 순서 밖이고, 안 기다리므로 순환도 안 만든다).
 * 만료만 반대였을 때 취소와 사이에 1213 이 나는 것을 재현했다
 * (MySQL 8.4 · READ COMMITTED · 두 세션 · 6/6).
 *
 * <p>저장소 메서드가 각각 어느 테이블을 잡는지는 이렇다.
 *
 * <table border="1">
 *   <caption>메서드와 잠그는 테이블 — 격리에 따라 다르다</caption>
 *   <tr><th></th><th>이 Step (READ COMMITTED)</th><th>REPEATABLE READ 였다면</th></tr>
 *   <tr><td>{@code nextCandidates}</td><td>없음</td><td>없음</td></tr>
 *   <tr><td>{@code lockStock}</td><td><b>coupon_stocks X</b></td><td>같음</td></tr>
 *   <tr><td>{@code expireBatch}</td><td>issuances X (매치 행)</td><td>같음</td></tr>
 *   <tr><td>{@code appendExpireHistories}</td><td>issuance_histories INSERT</td>
 *       <td>+ <b>issuances S</b></td></tr>
 *   <tr><td>{@code releaseStock}</td><td>coupon_stocks X (이미 우리 것)</td>
 *       <td>같음</td></tr>
 * </table>
 *
 * <p><b>RC 라 뒤 문장들이 {@code issuances} 에 락을 추가하지 않는다.</b>
 * {@code ExpirationLockScopeTest.eachStatementLocksTheTablesItsContractSays} 가 문장마다
 * 테이블별 락 수를 떠서 그것을 지킨다 — 첫 문장이 잡은 수에서 안 늘어나고,
 * {@code coupon_stocks} 는 <b>첫 쓰기 문장에서</b> 나타나고 끝까지 우리 것으로 남는다.
 *
 * <p><b>{@code issuance_histories} 칸만 재지 못한다.</b> INSERT 로 들어가는 행의 락은 InnoDB 가
 * 암묵적으로 잡아, 다른 세션이 부딪혀 실체화되기 전에는 {@code data_locks} 에 안 뜬다.
 * 그 자리는 아래 <b>호출 순서</b>가 맡는다 — 두 테스트가 합쳐야 표 전체가 선다.
 *
 * <p><b>발급 경로가 맞춰야 하는 것은 왼쪽 열이다.</b> 다만 격리가 RR 로 되돌아가면 오른쪽이
 * 되므로, 순서 계약({@code issuances} 먼저)은 어느 쪽에서도 지켜야 한다.
 * 오른쪽 열은 격리를 되돌려 본 값이 아니라 <b>InnoDB 문서가 말하는 동작</b>이다 —
 * 우리 청크에서는 첫 문장이 이미 X 락을 쥔 행이라 RR 로 바꿔도 락이 새로 안 생겼다.
 *
 * <p><b>반대로 잡으면 실제로 터진다.</b> {@code mysql:latest} 컨테이너에 두 세션을 띄워
 * 한쪽은 {@code issuances} → {@code coupon_stocks} 로, 다른 쪽은 그 반대로 잡게 했더니
 * 오류 <b>1213</b> 이 났고 한쪽이 통째로 되돌아갔다. 희생된 쪽은 만료였다 —
 * 되돌아가는 것이 만료면 그 주기의 만료가 통째로 밀린다.
 *
 * <p>여기서 보는 것은 <b>호출 순서</b>다. 데드락 자체를 테스트로 재현하면 어느 쪽이 희생될지가
 * 스케줄러에 달려 있어 결과가 흔들린다. 순서를 고정해 두면 그 뒤집힘이 여기서 먼저 드러난다.
 *
 * <p><b>다만 호출 순서는 락 순서의 증거가 아니다.</b> 한 문장이 두 테이블을 잡는 경우
 * ({@code releaseStock} 이 그렇다)를 원리적으로 못 본다 — 이 테스트만 있으면
 * {@code releaseStock} 이 안에서 {@code coupon_stocks} 를 먼저 잠가도 통과한다.
 * 그 축을 재는 것이 위에 적은 테이블별 락 테스트다. 예전에는 <i>"거기서 잰 값"</i> 이라고
 * 적어 뒀는데 <b>그 테스트는 {@code issuances} 만 세고 있었다</b> — 표를 뒷받침하는 것이
 * 아무것도 없었다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // 되읽기가 같은 ExpirationRepository 프록시에 countPending 을 부른다 —
        // 백그라운드 틱이 호출 기록에 끼면 재현 불가로 빨개진다.
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.expire.chunk-size=10"
})
@Import({ExpireJobLockOrderTest.FixedClockConfig.class, MySqlContainerConfig.class, ExpireJobLockOrderTest.RecordingConfig.class})
class ExpireJobLockOrderTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /** 고정한 "지금". 태스클릿이 asOf 를 이 값과 견주므로 벽시계면 실행 날짜에 딸린다. */
    private static final LocalDateTime NOW = AS_OF.plusDays(1);

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
        RecordingConfig.CALLS.clear();
    }

    @Test
    @DisplayName("한 청크가 coupon_stocks → issuances → issuance_histories 순으로 잡는다")
    void keepsLockOrderWithinChunk() throws Exception {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.minusDays(1))
                .param("id", id)
                .update();

        JobExecution execution = jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(RecordingConfig.CALLS)
                .as("재고를 나중에 잡으면 발급 경로와 순서가 역전돼 취소가 1213 으로 죽는다")
                .containsExactly(
                        // 순서 계약 밖이다 — 락을 안 잡기 때문이다. 다만 **그 성질을
                        // 지키는 것은 이 테스트가 아니다.** 여기는 이름의 순서만 보므로
                        // FOR SHARE 가 붙어도 초록이다. 실제로 재는 것은
                        // ExpirationLockScopeTest.readOnlyQueriesTakeNoLocks 다.
                        // 여기 적는 이유는 따로다 — 호출이 하나 는 것을 이 목록이 잡는다.
                        "blockedCoupons",
                        "nextCandidates",
                        // **여기가 이 테스트의 요지다.** 재고가 첫 쓰기 락이어야 한다.
                        "lockStock",
                        "expireBatch",
                        "appendExpireHistories",
                        "releaseStock",
                        // 후보가 없어 끝난다 — 종료 신호가 만료 0 이 아니라 후보 0 이다
                        "nextCandidates");
        // countPending 은 여기 없다 — CY-421 이 관측을 잡 밖의 되읽기로 옮겼다.
        // **잡이 자기 결과를 세지 않는 것이 요지다**: 그 값이 프로세스와 함께 죽으면
        // 만료가 일 1회인 지금 재기동부터 다음 창까지 백로그 감시가 통째로 꺼진다.
        // 그 호출은 이제 ExpirePendingRefresher 가 60초 주기로 하고,
        // ExpireMetricExposureTest 가 값을 잰다.
    }

    /** 저장소 호출 이름만 순서대로 적는다. 동작은 실제 저장소 그대로다. */
    @TestConfiguration
    static class RecordingConfig {

        static final List<String> CALLS = new CopyOnWriteArrayList<>();

        @Bean
        static BeanPostProcessor recordCalls() {
            return ExpirationProxies.decorating((real, method, args) -> {
                CALLS.add(method.getName());
                return ExpirationProxies.callThrough(real, method, args);
            });
        }
    }

    /**
     * <b>시계를 고정한다.</b> 이 테스트는 {@code expireJob} 을 실제로 돌리고, 태스클릿은
     * {@code asOf} 가 현재보다 미래면 {@code EXPIRE_ASOF_IN_FUTURE} 로 죽는다. 벽시계로 두면
     * 재는 축과 무관한 이유(<b>실행하는 날짜</b>)로 결과가 갈린다.
     *
     * <p>운영 {@code TimeConfig} 가 {@code systemUTC} 라는 것은 {@link FixedClock} 이 진다.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class FixedClockConfig {

        @Bean
        @Primary
        Clock fixedClock() {
            return FixedClock.at(NOW);
        }
    }
}
