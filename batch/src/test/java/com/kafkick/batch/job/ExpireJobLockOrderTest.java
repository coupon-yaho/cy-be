// 만료 잡이 테이블을 어떤 순서로 잠그는지 고정합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>락 순서는 혼자 지킬 수 없는 계약이다.</b> 만료가 어떤 순서로 잡든, 발급·사용·취소가
 * 반대로 잡으면 데드락이 난다. 그래서 <b>우리 순서를 먼저 고정해 두고</b> 상대가 맞추게 한다 —
 * 발급 경로는 아직 이 저장소에 없다.
 *
 * <p><b>순서는 {@code issuances} → {@code issuance_histories} → {@code coupon_stocks} 다.</b>
 * 저장소 메서드가 각각 어느 테이블을 잡는지는 이렇다.
 *
 * <table border="1">
 *   <caption>메서드와 잠그는 테이블 — 격리에 따라 다르다</caption>
 *   <tr><th></th><th>이 Step (READ COMMITTED)</th><th>REPEATABLE READ 였다면</th></tr>
 *   <tr><td>{@code expireBatch}</td><td>issuances X (매치 행)</td><td>같음</td></tr>
 *   <tr><td>{@code lastExpiredId}</td><td>없음</td><td>없음</td></tr>
 *   <tr><td>{@code appendExpireHistories}</td><td>issuance_histories INSERT</td>
 *       <td>+ <b>issuances S</b></td></tr>
 *   <tr><td>{@code expiredCouponCount}</td><td>없음</td><td>없음</td></tr>
 *   <tr><td>{@code stockRowCount}</td><td>없음</td><td>없음</td></tr>
 *   <tr><td>{@code releaseStock}</td><td>coupon_stocks X</td>
 *       <td>+ <b>issuances S</b></td></tr>
 * </table>
 *
 * <p><b>RC 라 뒤 문장들이 {@code issuances} 에 락을 추가하지 않는다.</b>
 * {@code ExpirationLockScopeTest.eachStatementLocksTheTablesItsContractSays} 가 문장마다
 * 테이블별 락 수를 떠서 그것을 지킨다 — 첫 문장이 잡은 수에서 안 늘어나고,
 * {@code coupon_stocks} 는 <b>마지막 문장에서만</b> 나타난다.
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
        "batch.expire.chunk-size=10"
})
@Import({MySqlContainerConfig.class, ExpireJobLockOrderTest.RecordingConfig.class})
class ExpireJobLockOrderTest {

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
        RecordingConfig.CALLS.clear();
    }

    @Test
    @DisplayName("한 청크가 issuances → issuance_histories → coupon_stocks 순으로 잡는다")
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
                .as("재고를 이력보다 먼저 잡으면 발급 경로와 순서가 역전돼 데드락이 난다")
                .containsExactly(
                        "expireBatch",
                        "lastExpiredId",
                        "appendExpireHistories",
                        "expiredCouponCount",
                        "stockRowCount",
                        "releaseStock",
                        // 남은 대상이 없어 0 을 돌려주고 끝난다
                        "expireBatch");
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
}
