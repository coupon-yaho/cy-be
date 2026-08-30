package com.kafkick.batch.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.batch.observation.ConsistencyRawValueReader;
import com.kafkick.batch.observation.DomainGaugeProperties;
import com.kafkick.core.consistency.ConsistencyRawValues;
import com.kafkick.core.consistency.ConsistencyRawSnapshot;
import com.kafkick.core.coupon.v2.port.GateMeta;
import com.kafkick.core.coupon.v2.port.GateStatus;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.coupon.v2.port.RebuiltIssued;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.infra.redis.coupon.v2.IssuanceKeys;
import com.kafkick.infra.redis.coupon.v2.IssuanceScriptRunner;
import com.kafkick.infra.redis.coupon.v2.RedisIssuanceGate;
import com.kafkick.infra.redis.coupon.v2.RedisIssuanceWarmup;
import com.kafkick.infra.redis.coupon.v2.RedisRestorationHaltStore;

/**
 * 워밍업을 <b>실제 MySQL 과 실제 Redis</b> 에 태운다. 둘 중 하나라도 대역이면 이 단위가 지는
 * 계약이 한 줄도 실행되지 않는다 — 집계 두 쿼리의 조건이 다르다는 것(활성은 CANCELLED 를
 * 빼고 회원 목록은 넣는다)도, {@code meta} 가 마지막이라는 것도 그렇다.
 *
 * <p><b>키는 {@link IssuanceKeys} 에서 온다.</b> 리터럴을 옮겨 적으면 어댑터가 키를 바꿔도
 * 여기는 계속 초록이고, 그 사실은 정합성 리더가 아무것도 못 읽을 때에야 드러난다.
 */
@ResourceLock(V2GateContainers.SHARED_STATE)
class CouponRoundWarmupRunnerTest {

    private static final long ROUND_ID = 500;
    private static final long OTHER_ROUND_ID = 501;
    private static final long TEMPLATE_ID = 50;
    private static final long OTHER_TEMPLATE_ID = 51;
    private static final long BRAND_ID = 5;
    private static final int TOTAL_QUANTITY = 100;
    private static final int GRADE_MASK = 1;

    /** 회차 오픈 전이다. 워밍업은 아직 열리지 않은 회차를 올리는 것이다. */
    private static final Instant NOW = Instant.parse("2026-08-28T00:00:00Z");
    private static final Instant OPEN_AT = Instant.parse("2026-08-28T01:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-08-28T02:00:00Z");

    private static final JdbcTemplate jdbc = V2GateContainers.jdbc();
    private static final TransactionTemplate transactionTemplate = V2GateContainers.transactions();
    private static final StringRedisTemplate redisTemplate = V2GateContainers.redis();

    private final IssuanceKeys keys = IssuanceKeys.of(ROUND_ID);

    private IssuanceGatePort gate;
    private IssuanceWarmupPort warmupPort;


    @BeforeEach
    void resetAndSeed() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
        jdbc.update("DELETE FROM issuances");
        jdbc.update("DELETE FROM coupon_stocks");
        jdbc.update("DELETE FROM coupons");
        jdbc.update("DELETE FROM coupon_templates");
        jdbc.update("DELETE FROM members");
        jdbc.update("DELETE FROM brands");
        jdbc.update("DELETE FROM grades");

        jdbc.update("INSERT INTO grades(code, bit_value) VALUES ('WELCOME', 1)");
        jdbc.update("INSERT INTO brands(id, name, category) VALUES (?, '브랜드', '카페')", BRAND_ID);
        insertTemplate(TEMPLATE_ID);
        insertTemplate(OTHER_TEMPLATE_ID);
        insertCoupon(ROUND_ID, TEMPLATE_ID, "V2");
        insertStock(ROUND_ID, TOTAL_QUANTITY, 0);

        gate = new RedisIssuanceGate(new IssuanceScriptRunner(redisTemplate), redisTemplate);
        warmupPort = new RedisIssuanceWarmup(
                redisTemplate, new RedisRestorationHaltStore(redisTemplate));
    }

    private CouponRoundWarmupRunner runner() {
        return runner(gate);
    }

    private CouponRoundWarmupRunner runner(IssuanceGatePort gatePort) {
        return runner(gatePort, warmupPort);
    }

    private CouponRoundWarmupRunner runner(IssuanceGatePort gatePort, IssuanceWarmupPort seeder) {
        return new CouponRoundWarmupRunner(
                new CouponRoundGateJdbc(jdbc, transactionTemplate),
                new RoundGateWriteGuard(), gatePort, seeder,
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)));
    }

    @Test
    @DisplayName("네 키 값이 DB 집계와 정확히 일치한다 — 활성은 CANCELLED 를 빼고 issued Hash 는 넣는다")
    void writesFourKeysMatchingDatabaseAggregates() {
        insertIssuance(1, ROUND_ID, 1, "ISSUED");
        insertIssuance(2, ROUND_ID, 2, "USED");
        insertIssuance(3, ROUND_ID, 3, "CANCELLED");
        insertIssuance(4, ROUND_ID, 4, "EXPIRED");
        // 다른 회차의 행은 이 회차의 집계에 들어오면 안 된다.
        insertCoupon(OTHER_ROUND_ID, OTHER_TEMPLATE_ID, "V2");
        insertStock(OTHER_ROUND_ID, 10, 0);
        insertIssuance(5, OTHER_ROUND_ID, 5, "ISSUED");

        CouponRoundWarmupResult result = runner().warmUp(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundWarmupStatus.WARMED);
        assertThat(result.activeCount()).isEqualTo(2);
        assertThat(result.issuedEverCount()).isEqualTo(4);
        assertThat(result.remainingStock()).isEqualTo(TOTAL_QUANTITY - 2);

        assertThat(redisTemplate.opsForValue().get(keys.stock()))
                .isEqualTo(Integer.toString(TOTAL_QUANTITY - 2));
        assertThat(redisTemplate.opsForValue().get(keys.issuedEver())).isEqualTo("4");
        assertThat(redisTemplate.opsForHash().size(keys.issued())).isEqualTo(4);
        assertThat(redisTemplate.opsForHash().keys(keys.issued()))
                .containsExactlyInAnyOrder("1", "2", "3", "4");
        assertThat(activeCountColumn(ROUND_ID)).isEqualTo(2);
    }

    @Test
    @DisplayName("meta 를 다섯 필드로 쓰고, 회차 정의가 그대로 실린다")
    void writesMetaWithFiveFields() {
        runner().warmUp(ROUND_ID);

        assertThat(redisTemplate.opsForHash().size(keys.meta())).isEqualTo(5);
        assertThat(gate.readMeta(ROUND_ID)).contains(new GateMeta(
                GateStatus.OPEN,
                OPEN_AT.toEpochMilli(),
                CLOSE_AT.toEpochMilli(),
                GRADE_MASK,
                TOTAL_QUANTITY));
    }

    @Test
    @DisplayName("meta 쓰기가 실패하면 세 키는 있고 meta 는 없다 — 게이트가 닫힌 채 남는다")
    void leavesGateClosedWhenMetaWriteFails() {
        insertIssuance(1, ROUND_ID, 1, "ISSUED");

        assertThatThrownBy(() -> runner(new FailingMetaGate(gate)).warmUp(ROUND_ID))
                .isInstanceOf(IllegalStateException.class);

        assertThat(redisTemplate.opsForHash().size(keys.issued())).isEqualTo(1);
        assertThat(redisTemplate.opsForValue().get(keys.issuedEver())).isEqualTo("1");
        assertThat(redisTemplate.opsForValue().get(keys.stock()))
                .isEqualTo(Integer.toString(TOTAL_QUANTITY - 1));
        assertThat(redisTemplate.hasKey(keys.meta())).isFalse();
    }

    @Test
    @DisplayName("meta 가 이미 있으면 아무것도 쓰지 않고 거절한다")
    void rejectsWhenGateAlreadyOpen() {
        gate.writeMeta(ROUND_ID, new GateMeta(GateStatus.OPEN, 1, 2, GRADE_MASK, TOTAL_QUANTITY));
        insertIssuance(1, ROUND_ID, 1, "ISSUED");

        CouponRoundWarmupResult result = runner().warmUp(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundWarmupStatus.GATE_ALREADY_OPEN);
        assertThat(redisTemplate.hasKey(keys.stock())).isFalse();
        assertThat(redisTemplate.hasKey(keys.issued())).isFalse();
        assertThat(activeCountColumn(ROUND_ID)).isZero();
    }

    @Test
    @DisplayName("V1 회차는 거절한다")
    void rejectsV1Round() {
        jdbc.update("UPDATE coupons SET issuance_engine_version = 'V1' WHERE id = ?", ROUND_ID);

        assertThat(runner().warmUp(ROUND_ID).status())
                .isEqualTo(CouponRoundWarmupStatus.ENGINE_NOT_V2);
        assertThat(redisTemplate.hasKey(keys.stock())).isFalse();
    }

    @Test
    @DisplayName("이미 오픈 시각을 지난 회차는 거절한다 — 살아있는 회차를 전환하지 않는다")
    void rejectsRoundAlreadyOpened() {
        jdbc.update("UPDATE coupons SET open_at = ? WHERE id = ?",
                utc(NOW.minusSeconds(1)), ROUND_ID);

        assertThat(runner().warmUp(ROUND_ID).status())
                .isEqualTo(CouponRoundWarmupStatus.ROUND_ALREADY_OPENED);
        assertThat(redisTemplate.hasKey(keys.stock())).isFalse();
    }

    @Test
    @DisplayName("게이트를 열기 전에 회차를 그 엔진에 잠근다 — 워밍업이 곧 엔진 확정이다")
    void locksIssuanceEngineBeforeOpeningGate() {
        assertThat(engineLocked(ROUND_ID)).isFalse();

        assertThat(runner().warmUp(ROUND_ID).status()).isEqualTo(CouponRoundWarmupStatus.WARMED);

        assertThat(engineLocked(ROUND_ID)).isTrue();
    }

    @Test
    @DisplayName("이미 잠긴 회차도 다시 워밍업된다 — meta 직전에 죽은 회차를 되살릴 수 있어야 한다")
    void warmsUpAlreadyLockedRound() {
        // 첫 실행이 잠금까지만 하고 meta 를 못 쓴 채 죽은 상태를 그대로 만든다.
        jdbc.update("UPDATE coupons SET issuance_engine_locked = TRUE WHERE id = ?", ROUND_ID);

        assertThat(runner().warmUp(ROUND_ID).status()).isEqualTo(CouponRoundWarmupStatus.WARMED);
        assertThat(gate.readMeta(ROUND_ID)).isPresent();
    }

    @Test
    @DisplayName("워밍업 도중 엔진이 V1 으로 뒤집히면 게이트를 열지 않는다")
    void rejectsWhenEngineFlipsDuringWarmup() {
        CouponRoundWarmupResult result = runner(gate, seederThatRuns(() ->
                jdbc.update("UPDATE coupons SET issuance_engine_version = 'V1' WHERE id = ?",
                        ROUND_ID))).warmUp(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundWarmupStatus.ENGINE_NOT_V2);
        // 세 키는 남지만 meta 가 없어 게이트는 닫힌 채다. 그게 안전한 상태다.
        assertThat(redisTemplate.hasKey(keys.meta())).isFalse();
        assertThat(engineLocked(ROUND_ID)).isFalse();
    }

    @Test
    @DisplayName("워밍업 도중 회차가 사라지면 엔진 문제와 구분해서 알린다")
    void distinguishesMissingRoundFromWrongEngine() {
        // coupon_stocks 가 coupons 를 FK 로 물고 있어 재고 행이 먼저다.
        CouponRoundWarmupResult result = runner(gate, seederThatRuns(() -> {
            jdbc.update("DELETE FROM coupon_stocks WHERE coupon_id = ?", ROUND_ID);
            jdbc.update("DELETE FROM coupons WHERE id = ?", ROUND_ID);
        })).warmUp(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundWarmupStatus.ROUND_NOT_FOUND);
        assertThat(redisTemplate.hasKey(keys.meta())).isFalse();
    }

    @Test
    @DisplayName("활성 건수가 총재고를 넘으면 거절한다 — DB 에 이미 초과 발급이 있다")
    void rejectsOverIssuedRound() {
        jdbc.update("UPDATE coupon_stocks SET total_quantity = 1 WHERE coupon_id = ?", ROUND_ID);
        insertIssuance(1, ROUND_ID, 1, "ISSUED");
        insertIssuance(2, ROUND_ID, 2, "USED");

        assertThat(runner().warmUp(ROUND_ID).status())
                .isEqualTo(CouponRoundWarmupStatus.OVER_ISSUED_ROUND);
        assertThat(redisTemplate.hasKey(keys.stock())).isFalse();
        assertThat(redisTemplate.hasKey(keys.meta())).isFalse();
    }

    @Test
    @DisplayName("같은 회차의 워밍업이 겹치면 늦은 쪽이 거절된다 — batch 1대가 이걸 막지 않는다")
    void rejectsConcurrentWarmupOfSameRound() throws Exception {
        insertIssuance(1, ROUND_ID, 1, "ISSUED");
        CountDownLatch seeding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // 먼저 들어온 쪽을 시딩 한가운데에 세워 둔다. HTTP 워커가 여럿이라 이 창은 실재한다.
        CouponRoundWarmupRunner runner = runner(gate, seederThatRuns(() -> {
            seeding.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<CouponRoundWarmupResult> first =
                    executor.submit(() -> runner.warmUp(ROUND_ID));
            assertThat(seeding.await(10, TimeUnit.SECONDS)).isTrue();

            CouponRoundWarmupResult second = runner.warmUp(ROUND_ID);

            assertThat(second.status()).isEqualTo(CouponRoundWarmupStatus.WARMUP_IN_PROGRESS);
            release.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS).status())
                    .isEqualTo(CouponRoundWarmupStatus.WARMED);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("회차 행이 없으면 거절한다")
    void rejectsMissingRound() {
        assertThat(runner().warmUp(9_999).status())
                .isEqualTo(CouponRoundWarmupStatus.ROUND_NOT_FOUND);
    }

    @Test
    @DisplayName("재고 행이 없으면 거절한다 — 총재고를 모르면 stock 도 meta 도 못 쓴다")
    void rejectsMissingStockRow() {
        jdbc.update("DELETE FROM coupon_stocks WHERE coupon_id = ?", ROUND_ID);

        assertThat(runner().warmUp(ROUND_ID).status())
                .isEqualTo(CouponRoundWarmupStatus.STOCK_ROW_MISSING);
        assertThat(redisTemplate.hasKey(keys.stock())).isFalse();
    }

    @Test
    @DisplayName("정합성 리더가 그 키들을 VALID 로 읽고 gap 네 축이 전부 0 이다")
    void consistencyReaderSeesValidKeysWithZeroGaps() {
        insertIssuance(1, ROUND_ID, 1, "ISSUED");
        insertIssuance(2, ROUND_ID, 2, "USED");
        insertIssuance(3, ROUND_ID, 3, "CANCELLED");
        insertIssuance(4, ROUND_ID, 4, "EXPIRED");

        runner().warmUp(ROUND_ID);

        // 리더는 batch 가 운영 중에 쓰는 그 코드다. 여기서 대역을 쓰면 워밍업이 올린 키를
        // 정작 관측이 못 읽는 조합이 초록으로 남는다.
        ConsistencyRawSnapshot observation = new ConsistencyRawValueReader(
                jdbc, redisTemplate,
                new DomainGaugeProperties(
                        EngineVersion.V2, ROUND_ID, null,
                        keys.stock(), keys.issuedEver(), keys.issued(), null),
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)))
                .read().consistency();

        assertThat(observation.redisObservation().status()).isEqualTo(SourceStatus.VALID);
        ConsistencyRawValues values = observation.rawValues();
        // §9.1 의 네 축을 식 그대로 적는다. 이름만 맞고 식이 틀리는 것을 막는다.
        assertThat(values.redisIssuedEverCount() - values.dbIssuedEverCount())
                .as("PERSIST_GAP").isZero();
        assertThat(values.redisIssuedEverCount() - values.redisMemberEverCount())
                .as("LUA_GAP").isZero();
        assertThat((values.totalQuantity() - values.redisRemaining()) - values.dbActiveCount())
                .as("ACTIVE_DB_GAP").isZero();
        assertThat(values.dbActiveCount() - values.storedActiveCount())
                .as("DB_COUNTER_GAP").isZero();
    }

    @Test
    @DisplayName("발급이 없는 새 회차는 재고 전부와 누적 0 으로 열린다")
    void warmsUpFreshRound() {
        CouponRoundWarmupResult result = runner().warmUp(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundWarmupStatus.WARMED);
        assertThat(redisTemplate.opsForValue().get(keys.stock()))
                .isEqualTo(Integer.toString(TOTAL_QUANTITY));
        assertThat(redisTemplate.opsForValue().get(keys.issuedEver())).isEqualTo("0");
        assertThat(gate.readMeta(ROUND_ID)).isPresent();
    }

    /** 시딩 한가운데에서 DB 를 흔든다. 잠금이 정말 meta 직전인지는 이걸로만 드러난다. */
    private IssuanceWarmupPort seederThatRuns(Runnable duringSeeding) {
        return new IssuanceWarmupPort() {

            @Override
            public void seedCounters(
                    long roundId, List<RebuiltIssued> members, long remaining) {
                duringSeeding.run();
                warmupPort.seedCounters(roundId, members, remaining);
            }

            @Override
            public void setRemainingStock(long roundId, long remaining) {
                warmupPort.setRemainingStock(roundId, remaining);
            }
        };
    }

    private boolean engineLocked(long couponRoundId) {
        return Boolean.TRUE.equals(jdbc.queryForObject(
                "SELECT issuance_engine_locked FROM coupons WHERE id = ?",
                Boolean.class, couponRoundId));
    }

    // ── 픽스처 ────────────────────────────────────────────────────────────────

    /** {@code meta} 만 실패시킨다. 게이트를 여는 단계가 정말 마지막인지는 이걸로만 드러난다. */
    private record FailingMetaGate(IssuanceGatePort delegate) implements IssuanceGatePort {

        @Override
        public com.kafkick.core.coupon.v2.port.ClaimResult claim(
                com.kafkick.core.coupon.v2.port.ClaimCommand command) {
            return delegate.claim(command);
        }

        @Override
        public com.kafkick.core.coupon.v2.port.CompleteOutcome complete(
                long couponRoundId, long memberId, String requestToken) {
            return delegate.complete(couponRoundId, memberId, requestToken);
        }

        @Override
        public com.kafkick.core.coupon.v2.port.CompensateOutcome compensate(
                long couponRoundId, long memberId, String requestToken) {
            return delegate.compensate(couponRoundId, memberId, requestToken);
        }

        @Override
        public com.kafkick.core.coupon.v2.port.RestoreOutcome restore(long couponRoundId, long count) {
            return delegate.restore(couponRoundId, count);
        }

        @Override
        public com.kafkick.core.coupon.v2.port.ReclaimOutcome reclaimCorrupt(
                long couponRoundId, long memberId, boolean restoreStock, long totalQuantity) {
            return delegate.reclaimCorrupt(couponRoundId, memberId, restoreStock, totalQuantity);
        }

        @Override
        public void closeGate(long couponRoundId) {
            delegate.closeGate(couponRoundId);
        }

        @Override
        public void writeMeta(long couponRoundId, GateMeta meta) {
            throw new IllegalStateException("meta 쓰기 실패");
        }

        @Override
        public Optional<GateMeta> readMeta(long couponRoundId) {
            return delegate.readMeta(couponRoundId);
        }
    }

    private int activeCountColumn(long couponRoundId) {
        return jdbc.queryForObject(
                "SELECT active_count FROM coupon_stocks WHERE coupon_id = ?",
                Integer.class, couponRoundId);
    }

    private static void insertTemplate(long templateId) {
        jdbc.update("""
                INSERT INTO coupon_templates(
                    id, brand_id, name, policy_type, valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence, eligible_grades_mask,
                    active, created_at, updated_at)
                VALUES (?, ?, '템플릿', 'FIXED_AMOUNT', 30, 1, 'MON', '10:00:00', 1, ?, ?, true, ?, ?)
                """, templateId, BRAND_ID, TOTAL_QUANTITY, GRADE_MASK, utc(NOW), utc(NOW));
    }

    private static void insertCoupon(long id, long templateId, String engineVersion) {
        jdbc.update("""
                INSERT INTO coupons(
                    id, template_id, brand_id, name, policy_type, valid_days,
                    eligible_grades_mask, open_at, close_at, status, generated_at, created_at,
                    issuance_engine_version)
                VALUES (?, ?, ?, ?, 'FIXED_AMOUNT', 30, ?, ?, ?, 'SCHEDULED', ?, ?, ?)
                """, id, templateId, BRAND_ID, "회차 " + id, GRADE_MASK,
                utc(OPEN_AT), utc(CLOSE_AT), utc(NOW), utc(NOW), engineVersion);
    }

    private static void insertStock(long couponId, int totalQuantity, int activeCount) {
        jdbc.update("""
                INSERT INTO coupon_stocks(coupon_id, total_quantity, active_count, updated_at)
                VALUES (?, ?, ?, ?)
                """, couponId, totalQuantity, activeCount, utc(NOW));
    }

    private static void insertIssuance(long id, long couponId, long memberId, String status) {
        jdbc.update("INSERT INTO members(id, membership_grade, created_at) VALUES (?, 'WELCOME', ?)",
                memberId, utc(NOW.minusSeconds(86_400)));
        jdbc.update("""
                INSERT INTO issuances(
                    id, coupon_id, member_id, code, issued_grade, status,
                    issued_at, expires_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'WELCOME', ?, ?, ?, ?, ?)
                """, id, couponId, memberId, String.format("C%015d", id), status,
                utc(NOW.minusSeconds(600)), utc(NOW.plusSeconds(86_400)),
                utc(NOW.minusSeconds(600)), utc(NOW.minusSeconds(600)));
    }

    private static LocalDateTime utc(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
