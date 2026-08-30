package com.kafkick.batch.job;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.v2.V2StockRestorationService;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>만료가 V2 회차의 Redis 재고를 되돌리는가</b>(설계 §5 · CY-750).
 *
 * <p>이 검사가 있어야 하는 이유는 이력이다. CY-750 은 옛 만료 구현
 * ({@code CouponExpirationService})에 이 호출을 넣었는데, main 이 만료 배치를 Spring Batch 로
 * 다시 만들면서 그 파일을 지웠다. 머지에서 <b>호출이 통째로 사라져도 컴파일은 통과한다</b> —
 * 그 사실은 부하 시험 끝에 {@code ACTIVE_DB_GAP} 이 안 닫힐 때에야 드러난다.
 *
 * <p>그래서 여기서 보는 것은 값이 아니라 <b>호출의 존재와 인자</b>다. 실제 Redis 왕복은
 * {@code V2StockRestorationServiceTest} 가 이미 덮는다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.expire.chunk-size=10",
        "coupon.rebuild.drain=2s"
})
@Import(MySqlContainerConfig.class)
class ExpireV2StockRestorationTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job expireJob;

    @Autowired
    private JdbcClient jdbcClient;

    /** 실물을 물리면 Redis 가 없어 조립부터 터진다. 여기서 보는 것은 호출 자체다. */
    @MockitoBean
    private V2StockRestorationService v2StockRestoration;

    private VerificationSeed seed;

    @BeforeEach
    void setUp() {
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
    }

    @Test
    @DisplayName("V2 회차의 만료는 Redis 재고 복원을 만료 건수만큼 등록한다")
    void registersRedisRestorationForV2Round() throws Exception {
        seed.issuance(IssuanceStatus.ISSUED);
        seed.issuance(IssuanceStatus.ISSUED);
        setEngine("V2");

        launch();

        verify(v2StockRestoration).restoreAfterCommit(seed.currentCouponId(), 2L);
    }

    @Test
    @DisplayName("만료할 것이 없으면 복원을 등록하지 않는다 — 0 건에 대고 부르지 않는다")
    void skipsRestorationWhenNothingExpired() throws Exception {
        seed.issuance(IssuanceStatus.USED);
        setEngine("V2");

        launch();

        verify(v2StockRestoration, never()).restoreAfterCommit(anyLong(), anyLong());
    }

    /**
     * V1 회차도 <b>호출은 한다.</b> 엔진 판별은 서비스 안에서 하고, 배치는 그것을 모른다 —
     * 그 경계가 뒤집히면(배치가 엔진을 보고 거르면) 판별이 두 곳으로 갈린다.
     */
    @Test
    @DisplayName("V1 회차도 호출은 한다 — 엔진 판별은 복원 서비스의 몫이다")
    void callsEvenForV1AndLetsTheServiceDecide() throws Exception {
        seed.issuance(IssuanceStatus.ISSUED);
        setEngine("V1");

        launch();

        verify(v2StockRestoration).restoreAfterCommit(seed.currentCouponId(), 1L);
    }

    private void setEngine(String engineVersion) {
        jdbcClient.sql("UPDATE coupons SET issuance_engine_version = :v WHERE id = :id")
                .param("v", engineVersion)
                .param("id", seed.currentCouponId())
                .update();
    }

    private void launch() throws Exception {
        jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());
    }
}
