package com.kafkick.batch.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Driver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.coupon.v2.port.ClaimCommand;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.GateMeta;
import com.kafkick.core.coupon.v2.port.GateStatus;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.coupon.v2.port.RebuiltIssued;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.infra.redis.coupon.v2.IssuanceKeys;
import com.kafkick.infra.redis.coupon.v2.IssuanceScriptRunner;
import com.kafkick.infra.redis.coupon.v2.RedisIssuanceGate;
import com.kafkick.infra.redis.coupon.v2.RedisIssuanceWarmup;
import com.kafkick.infra.redis.coupon.v2.RedisRestorationHaltStore;

/**
 * 재구성을 <b>실제 MySQL 과 실제 Redis</b> 에 태운다. 이 단위가 지는 계약은 전부 순서에 있어,
 * 둘 중 하나라도 대역이면 한 줄도 실행되지 않는다 — 게이트가 <b>먼저</b> 닫히는가,
 * {@code meta} 가 <b>마지막</b>인가, 그리고 <b>4′ 가 정말 다시 세는가</b>.
 *
 * <p>회차는 <b>이미 열려 있다</b>. 워밍업 테스트가 오픈 전 회차를 쓰는 것과 여기가 갈리는
 * 지점이고, 그것이 이 경로의 존재 이유다.
 */
class CouponRoundRebuildRunnerTest {

    private static final long ROUND_ID = 700;
    private static final long TEMPLATE_ID = 70;
    private static final long BRAND_ID = 7;
    private static final int TOTAL_QUANTITY = 100;
    private static final int GRADE_MASK = 1;
    private static final int GRADE_BIT = 1;

    /**
     * <b>회차가 열려 있다.</b> 워밍업이 손대지 못하는 상태가 재구성의 정상 입력이다.
     *
     * <p>고정 시각이 아니라 <b>실행 시각 기준</b>이다. 선점 Lua 는 회차의 열림·마감을
     * {@code meta} 와 <b>Redis 서버의 {@code TIME}</b> 으로 판정하므로(시각의 원본은 Redis
     * 하나다), 박아 둔 날짜를 쓰면 그 날이 지나는 순간 "재구성 뒤 발급이 된다" 를 보는 검사가
     * 조용히 {@code -3}(마감)으로 바뀐다.
     */
    // 초 단위로 자른다 — DATETIME 컬럼이 소수점 이하를 버려서, 안 자르면 meta 에 실린
    // epochMillis 가 DB 에서 읽어 온 값과 갈린다.
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    private static final Instant OPEN_AT = NOW.minusSeconds(1_800);
    private static final Instant CLOSE_AT = NOW.plusSeconds(5_400);

    private static MySQLContainer mysql;
    private static GenericContainer<?> redis;
    private static JdbcTemplate jdbc;
    private static TransactionTemplate transactionTemplate;
    private static LettuceConnectionFactory redisFactory;
    private static StringRedisTemplate redisTemplate;

    private final IssuanceKeys keys = IssuanceKeys.of(ROUND_ID);

    private IssuanceGatePort gate;
    private IssuanceWarmupPort warmupPort;
    private RoundGateWriteGuard guard;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void startContainers() throws Exception {
        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                .withDatabaseName("app")
                .withCommand("--default-time-zone=+00:00");
        mysql.start();
        Flyway.configure()
                .dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
        SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        dataSource.setDriverClass(
                (Class<? extends Driver>) Class.forName(mysql.getDriverClassName()));
        dataSource.setUrl(mysql.getJdbcUrl());
        dataSource.setUsername(mysql.getUsername());
        dataSource.setPassword(mysql.getPassword());
        jdbc = new JdbcTemplate(dataSource);
        transactionTemplate = new TransactionTemplate(new JdbcTransactionManager(dataSource));

        redis = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
                .withExposedPorts(6379);
        redis.start();
        redisFactory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(redis.getHost(), redis.getFirstMappedPort()));
        redisFactory.afterPropertiesSet();
        redisFactory.start();
        redisTemplate = new StringRedisTemplate(redisFactory);
    }

    @AfterAll
    static void stopContainers() {
        if (redisFactory != null) {
            redisFactory.destroy();
        }
        if (mysql != null) {
            mysql.stop();
        }
        if (redis != null) {
            redis.stop();
        }
    }

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
        insertTemplate();
        insertCoupon(ROUND_ID, "V2");
        insertStock(ROUND_ID, TOTAL_QUANTITY, 0);

        gate = new RedisIssuanceGate(new IssuanceScriptRunner(redisTemplate), redisTemplate);
        warmupPort = new RedisIssuanceWarmup(
                redisTemplate, new RedisRestorationHaltStore(redisTemplate));
        guard = new RoundGateWriteGuard();
    }

    private CouponRoundRebuildRunner runner() {
        return runner(warmupPort);
    }

    private CouponRoundRebuildRunner runner(IssuanceWarmupPort seeder) {
        return runner(seeder, Duration.ZERO);
    }

    /** 대기는 기본이 0 이다 — 대기 자체를 보는 테스트만 값을 준다. */
    private CouponRoundRebuildRunner runner(IssuanceWarmupPort seeder, Duration drain) {
        return new CouponRoundRebuildRunner(
                new CouponRoundGateJdbc(jdbc, transactionTemplate), guard, gate, seeder,
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)), drain);
    }

    /** 게이트가 열린 채 카운터만 낡아 있는 상태 — Sentinel failover 뒤의 그림이다. */
    private void openGateWithStaleCounters() {
        gate.writeMeta(ROUND_ID, new GateMeta(GateStatus.OPEN, OPEN_AT.toEpochMilli(),
                CLOSE_AT.toEpochMilli(), GRADE_MASK, TOTAL_QUANTITY));
        redisTemplate.opsForValue().set(keys.stock(), "999");
        redisTemplate.opsForValue().set(keys.issuedEver(), "77");
        redisTemplate.opsForHash().put(keys.issued(), "999", "P|1|stale|stale");
    }

    @Test
    @DisplayName("열린 회차를 다시 세운다 — 네 키가 DB 집계와 정확히 일치하고 HLEN == issued_ever 다")
    void rebuildsOpenRoundToMatchDatabaseAggregates() {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");
        insertIssuance(2, 2, "USED");
        insertIssuance(3, 3, "CANCELLED");
        insertIssuance(4, 4, "EXPIRED");

        CouponRoundRebuildResult result = runner().rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.REBUILT);
        assertThat(result.activeCount()).isEqualTo(2);
        assertThat(result.issuedEverCount()).isEqualTo(4);
        assertThat(result.remainingStock()).isEqualTo(TOTAL_QUANTITY - 2);

        assertThat(redisTemplate.opsForValue().get(keys.stock()))
                .isEqualTo(Integer.toString(TOTAL_QUANTITY - 2));
        assertThat(redisTemplate.opsForValue().get(keys.issuedEver())).isEqualTo("4");
        assertThat(redisTemplate.opsForHash().keys(keys.issued()))
                .containsExactlyInAnyOrder("1", "2", "3", "4");
        // 어긋나면 그게 곧 LUA_GAP 이다.
        assertThat(redisTemplate.opsForHash().size(keys.issued()))
                .isEqualTo(Long.parseLong(redisTemplate.opsForValue().get(keys.issuedEver())));
        assertThat(activeCountColumn()).isEqualTo(2);
        assertThat(gate.readMeta(ROUND_ID)).contains(new GateMeta(
                GateStatus.OPEN, OPEN_AT.toEpochMilli(), CLOSE_AT.toEpochMilli(),
                GRADE_MASK, TOTAL_QUANTITY));
    }

    @Test
    @DisplayName("재구성 중에는 그 회차 발급이 전량 -9 다 — 게이트가 먼저 닫힌다")
    void closesGateBeforeRewritingCounters() {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");

        CouponRoundRebuildResult result = runner(seederThatRuns(() -> {
            assertThat(gate.readMeta(ROUND_ID)).isEmpty();
            assertThat(claim(500).outcome()).isEqualTo(ClaimOutcome.GATE_NOT_READY);
        })).rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.REBUILT);
        assertThat(gate.readMeta(ROUND_ID)).isPresent();
    }

    @Test
    @DisplayName("도중에 죽으면 meta 가 없는 상태로 남고, 다시 돌리면 복구된다")
    void leavesGateClosedOnFailureAndRecoversOnRerun() {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");

        // 게이트를 닫은 뒤의 실패는 예외로 나가지 않는다 — 그러면 500 이 되어 "게이트가
        // 닫혔다" 는 사실이 응답에서 사라진다. 결과로 나가고, 게이트는 닫힌 채다.
        CouponRoundRebuildResult failed = runner(seederThatRuns(() -> {
            throw new IllegalStateException("시딩 도중 죽었다");
        })).rebuild(ROUND_ID);

        assertThat(failed.status()).isEqualTo(CouponRoundRebuildStatus.REBUILD_FAILED);
        assertThat(failed.gateClosed()).isTrue();
        assertThat(gate.readMeta(ROUND_ID)).isEmpty();

        CouponRoundRebuildResult rerun = runner().rebuild(ROUND_ID);

        assertThat(rerun.status()).isEqualTo(CouponRoundRebuildStatus.REBUILT);
        assertThat(gate.readMeta(ROUND_ID)).isPresent();
        assertThat(redisTemplate.opsForValue().get(keys.stock()))
                .isEqualTo(Integer.toString(TOTAL_QUANTITY - 1));
    }

    /**
     * <b>이 티켓의 핵심이다.</b> 게이트는 발급만 막는다 — 취소는 창 안에서도 커밋된다.
     * 2번 집계 이후 커밋된 취소가 {@code stock} 에 반영되지 않으면 그만큼 재고가 조용히
     * 유실된다(§6.2 가 적어 놓은 구멍).
     */
    @Test
    @DisplayName("4′ — 집계 이후 커밋된 취소가 stock 에 반영된다")
    void recountsActiveRowsCommittedInsideTheWindow() {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");
        insertIssuance(2, 2, "ISSUED");
        insertIssuance(3, 3, "ISSUED");

        // 집계는 끝났고 meta 는 아직 없다. 그 창에서 취소가 커밋된다.
        CouponRoundRebuildResult result = runner(seederThatRuns(
                () -> jdbc.update("UPDATE issuances SET status = 'CANCELLED' WHERE id = 3")))
                .rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.REBUILT);
        assertThat(result.activeCount()).isEqualTo(3);
        assertThat(result.recountedActiveCount()).isEqualTo(2);
        assertThat(result.remainingStock()).isEqualTo(TOTAL_QUANTITY - 2);

        assertThat(redisTemplate.opsForValue().get(keys.stock()))
                .isEqualTo(Integer.toString(TOTAL_QUANTITY - 2));
        assertThat(activeCountColumn()).isEqualTo(2);
        // 누적은 취소로 줄지 않는다 — 1인 1매가 평생 기준이라 세 회원 모두 남는다.
        assertThat(redisTemplate.opsForValue().get(keys.issuedEver())).isEqualTo("3");
        assertThat(redisTemplate.opsForHash().size(keys.issued())).isEqualTo(3);
    }

    /**
     * 게이트는 <b>새 선점만</b> 막는다. 닫기 직전에 선점을 끝낸 발급의 {@code issuances} 커밋은
     * 그 뒤에 도착하는데(발급은 선점 → DB 커밋 → 완료 순서다), 그 커밋이 집계보다 늦으면 그
     * 회원은 재작성된 {@code issued} 에서 통째로 빠진다 — 그것이 곧 {@code LUA_GAP} 이고,
     * 재오픈 뒤 같은 회원의 재선점이 Lua 를 통과한다.
     */
    @Test
    @DisplayName("게이트를 닫은 뒤 기다린다 — 닫는 순간 진행 중이던 발급의 커밋까지 집계에 든다")
    void waitsForInFlightWritesBeforeReadingAggregates() throws Exception {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");
        CouponRoundRebuildRunner rebuildRunner = runner(warmupPort, Duration.ofSeconds(1));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            // 게이트가 닫힌 뒤에 커밋되는 발급 — 대기가 없으면 집계가 이 행을 못 본다.
            Future<CouponRoundRebuildResult> late = executor.submit(() -> {
                Thread.sleep(200);
                insertIssuance(2, 2, "ISSUED");
                return null;
            });

            CouponRoundRebuildResult result = rebuildRunner.rebuild(ROUND_ID);
            late.get(10, TimeUnit.SECONDS);

            assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.REBUILT);
            assertThat(result.issuedEverCount()).isEqualTo(2);
            assertThat(redisTemplate.opsForHash().keys(keys.issued()))
                    .containsExactlyInAnyOrder("1", "2");
            assertThat(redisTemplate.opsForValue().get(keys.stock()))
                    .isEqualTo(Integer.toString(TOTAL_QUANTITY - 2));
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * 4′ 는 활성 건수를 <b>절대값</b>으로 쓴다. 세는 것과 쓰는 것이 다른 트랜잭션이면 그 사이에
     * 커밋된 취소의 감소분을 그 절대값이 덮어 없앤다 — 재구성이 정리하겠다고 만든
     * {@code DB_COUNTER_GAP}(§9.1 I6)이 재구성 <b>직후부터</b> 0 이 아니게 된다.
     *
     * <p>재고 행을 먼저 잠그므로, 여기서 취소 트랜잭션은 우리보다 <b>먼저</b> 끝나고 그 결과가
     * 집계에 든다.
     */
    @Test
    @DisplayName("4′ 재집계 중에 커밋된 취소의 감소분을 덮어쓰지 않는다")
    void doesNotOverwriteACancelCommittedWhileRecounting() {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");
        insertIssuance(2, 2, "ISSUED");
        insertIssuance(3, 3, "ISSUED");
        jdbc.update("UPDATE coupon_stocks SET active_count = 3 WHERE coupon_id = ?", ROUND_ID);
        CountDownLatch holdingStockRow = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            CouponRoundRebuildResult result = runner(seederThatRuns(() -> {
                // 취소 한 건이 재고 행을 잡은 채 잠시 머문다 — 4′ 가 그 위로 지나간다.
                executor.submit(() -> transactionTemplate.execute(status -> {
                    jdbc.update("UPDATE issuances SET status = 'CANCELLED' WHERE id = 3");
                    jdbc.update("UPDATE coupon_stocks SET active_count = active_count - 1"
                            + " WHERE coupon_id = ?", ROUND_ID);
                    holdingStockRow.countDown();
                    sleepQuietly(700);
                    return null;
                }));
                await(holdingStockRow);
            })).rebuild(ROUND_ID);

            assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.REBUILT);
            assertThat(result.recountedActiveCount()).isEqualTo(2);
            assertThat(activeCountColumn()).isEqualTo(2);
            assertThat(redisTemplate.opsForValue().get(keys.stock()))
                    .isEqualTo(Integer.toString(TOTAL_QUANTITY - 2));
        } finally {
            executor.shutdown();
        }
    }

    @Test
    @DisplayName("워밍업과 가드를 공유한다 — 재구성이 도는 회차의 워밍업은 거절된다")
    void sharesTheInFlightGuardWithWarmup() throws Exception {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");
        CountDownLatch seeding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CouponRoundRebuildRunner rebuildRunner = runner(seederThatRuns(() -> {
            seeding.countDown();
            await(release);
        }));
        CouponRoundWarmupRunner warmupRunner = new CouponRoundWarmupRunner(
                new CouponRoundGateJdbc(jdbc, transactionTemplate), guard, gate, warmupPort,
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<CouponRoundRebuildResult> first =
                    executor.submit(() -> rebuildRunner.rebuild(ROUND_ID));
            assertThat(seeding.await(10, TimeUnit.SECONDS)).isTrue();

            // 게이트가 닫혀 있는 창이라, 가드가 없으면 워밍업이 "안 열린 회차" 로 보고 통과한다.
            assertThat(warmupRunner.warmUp(ROUND_ID).status())
                    .isEqualTo(CouponRoundWarmupStatus.WARMUP_IN_PROGRESS);
            assertThat(rebuildRunner.rebuild(ROUND_ID).status())
                    .isEqualTo(CouponRoundRebuildStatus.REBUILD_IN_PROGRESS);

            release.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS).status())
                    .isEqualTo(CouponRoundRebuildStatus.REBUILT);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    /**
     * 4′ 재집계에서 <b>처음</b> 초과가 드러나는 경우다. drain 보다 늦게 커밋된 선점분이 그
     * 창을 만든다 — v2 의 {@code incrementActiveCount} 에는 {@code active_count <
     * total_quantity} 가드가 없어(재고의 권위가 Redis 라 의도된 것) DB 는 그 커밋을 막지 않는다.
     *
     * <p>여기서 게이트를 열면 {@code stock} 이 음수인 채로 열린다. 선점 Lua 는 음수를
     * {@code -5}(매진)로 읽어 <b>초과 발급을 정상 상태로 굳히고</b>, 취소가 들어와도
     * {@code INCR} 이 0 을 넘길 때까지 재고가 한 장도 안 돌아온다.
     */
    @Test
    @DisplayName("4′ 재집계에서 초과가 드러나면 meta 를 쓰지 않는다 — 음수 stock 위에 게이트를 열지 않는다")
    void refusesToOpenTheGateWhenTheRecountShowsOverIssuance() {
        jdbc.update("UPDATE coupon_stocks SET total_quantity = 3 WHERE coupon_id = ?", ROUND_ID);
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");
        insertIssuance(2, 2, "ISSUED");
        insertIssuance(3, 3, "ISSUED");

        // 집계(활성 3 = 총재고 3)를 통과한 뒤, drain 을 넘겨 도착한 선점분이 커밋된다.
        CouponRoundRebuildResult result = runner(seederThatRuns(
                () -> insertIssuance(4, 4, "ISSUED"))).rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.OVER_ISSUED_ROUND);
        // 게이트는 닫힌 채다. 그 회차는 전면 503 이고, 그것이 초과 발급을 굳히는 것보다 낫다.
        assertThat(gate.readMeta(ROUND_ID)).isEmpty();
        // 음수가 Redis 에 닿지도 않는다.
        assertThat(Long.parseLong(redisTemplate.opsForValue().get(keys.stock()))).isNotNegative();
        // 복구에 필요한 수치는 결과에 남는다 — 로그를 뒤지게 하지 않는다.
        assertThat(result.totalQuantity()).isEqualTo(3);
        assertThat(result.recountedActiveCount()).isEqualTo(4);
    }

    @Test
    @DisplayName("이미 초과 발급된 회차는 게이트를 닫기 전에 거절한다")
    void rejectsOverIssuedRoundWithoutClosingTheGate() {
        openGateWithStaleCounters();
        jdbc.update("UPDATE coupon_stocks SET total_quantity = 1 WHERE coupon_id = ?", ROUND_ID);
        insertIssuance(1, 1, "ISSUED");
        insertIssuance(2, 2, "ISSUED");

        CouponRoundRebuildResult result = runner().rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.OVER_ISSUED_ROUND);
        // 멀쩡히 돌던 회차를 세우지 않는다.
        assertThat(gate.readMeta(ROUND_ID)).isPresent();
        assertThat(redisTemplate.opsForValue().get(keys.stock())).isEqualTo("999");
        assertThat(result.totalQuantity()).isEqualTo(1);
        assertThat(result.activeCount()).isEqualTo(2);
    }

    /**
     * 게이트를 닫은 <b>뒤에</b> 끝나는 거절은 그 회차를 전면 503 으로 남긴다. 닫기 전에 끝나는
     * 거절과 같은 얼굴로 나가면 운영자는 "아무것도 안 건드렸구나" 하고 손을 뗀다 — 그러면
     * 그 503 은 다음 재구성까지 남는다.
     */
    @Test
    @DisplayName("게이트를 닫은 뒤 끝나는 거절은 gateClosed 로 구분된다 — 재고 행이 사라진 경우")
    void marksRejectionsThatLeaveTheGateClosed() {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");
        // 게이트를 닫는 순간과 집계 사이에 재고 행이 사라진다. drain 이 그 창을 넓힌다.
        IssuanceGatePort deletingGate = new DelegatingGate(gate) {
            @Override
            public void closeGate(long couponRoundId) {
                super.closeGate(couponRoundId);
                jdbc.update("DELETE FROM coupon_stocks WHERE coupon_id = ?", couponRoundId);
            }
        };

        CouponRoundRebuildResult result = new CouponRoundRebuildRunner(
                new CouponRoundGateJdbc(jdbc, transactionTemplate), guard, deletingGate,
                warmupPort, new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)), Duration.ZERO)
                .rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.STOCK_ROW_MISSING);
        assertThat(result.gateClosed()).isTrue();
        assertThat(gate.readMeta(ROUND_ID)).isEmpty();
    }

    @Test
    @DisplayName("게이트를 닫기 전에 끝나는 거절은 gateClosed 가 아니다")
    void marksRejectionsThatLeftTheGateAlone() {
        openGateWithStaleCounters();
        jdbc.update("DELETE FROM coupon_stocks WHERE coupon_id = ?", ROUND_ID);

        CouponRoundRebuildResult result = runner().rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.STOCK_ROW_MISSING);
        assertThat(result.gateClosed()).isFalse();
        assertThat(gate.readMeta(ROUND_ID)).isPresent();
    }

    /**
     * drain 을 넘겨 커밋된 선점은 {@code everMembers} 에 없고, 시딩은 {@code issued} 를 통째로
     * 덮는다 — 그 회원의 Redis 층 1인 1매 방어가 사라지고 {@code uk_coupon_member} 만 남는다.
     * 게이트가 아직 닫혀 있는 동안이 그것을 고칠 수 있는 마지막 순간이다.
     */
    @Test
    @DisplayName("시딩 뒤에 커밋된 발급을 최신 목록으로 다시 시딩해 메운다")
    void reseedsOnceWhenIssuancesLandAfterSeeding() {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");
        AtomicBoolean first = new AtomicBoolean(true);

        CouponRoundRebuildResult result = runner(seederThatRuns(() -> {
            if (first.getAndSet(false)) {
                insertIssuance(2, 2, "ISSUED");
            }
        })).rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.REBUILT);
        // 늦게 온 회원까지 issued 에 들어간다 — Redis 층의 1인 1매 방어가 되살아난다.
        assertThat(redisTemplate.opsForHash().keys(keys.issued()))
                .containsExactlyInAnyOrder("1", "2");
        assertThat(redisTemplate.opsForValue().get(keys.issuedEver())).isEqualTo("2");
        assertThat(result.issuedEverCount()).isEqualTo(2);
        assertThat(result.recountedIssuedEverCount()).isEqualTo(2);
        assertThat(result.issuedHashIsShort()).isFalse();
        assertThat(result.remainingStock()).isEqualTo(TOTAL_QUANTITY - 2);
    }

    /**
     * 재시딩은 <b>한 번만</b> 돈다. 그보다도 늦은 커밋은 남고, 그때 재구성이 스스로 말하지
     * 않으면 신호가 사후 {@code LUA_GAP} 하나뿐이다.
     */
    @Test
    @DisplayName("다시 시딩한 뒤에도 늦게 커밋되면 몇 건인지 보고하고 게이트는 연다")
    void reportsIssuancesThatOutrunTheReseed() {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");
        AtomicInteger nextId = new AtomicInteger(2);

        // 시딩할 때마다 한 건씩 더 커밋된다 — 재시딩도 따라잡지 못하는 상황이다.
        CouponRoundRebuildResult result = runner(seederThatRuns(() -> {
            int id = nextId.getAndIncrement();
            insertIssuance(id, id, "ISSUED");
        })).rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.REBUILT);
        assertThat(result.issuedEverCount()).isEqualTo(2);
        assertThat(result.recountedIssuedEverCount()).isEqualTo(3);
        assertThat(result.issuedHashIsShort()).isTrue();
        // 게이트는 연다 — 닫아 두면 부하 중에는 영원히 수렴하지 못한다.
        assertThat(gate.readMeta(ROUND_ID)).isPresent();
    }

    /**
     * 게이트를 닫은 뒤의 <b>예외</b>도 같은 계약을 진다. 그대로 흘려보내면 500 이 나가고,
     * 그 순간 "게이트가 닫혔다" 는 사실이 응답에서 사라진다.
     */
    @Test
    @DisplayName("게이트를 닫은 뒤 예외로 멈춰도 500 이 아니라 gateClosed 로 나간다")
    void reportsUnexpectedFailuresAfterCloseAsGateClosed() {
        openGateWithStaleCounters();
        insertIssuance(1, 1, "ISSUED");

        // 집계는 끝났고 meta 는 없다. 그 창에서 재고 행이 사라져 4′ 가 터진다.
        CouponRoundRebuildResult result = runner(seederThatRuns(
                () -> jdbc.update("DELETE FROM coupon_stocks WHERE coupon_id = ?", ROUND_ID)))
                .rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.REBUILD_FAILED);
        assertThat(result.gateClosed()).isTrue();
        assertThat(gate.readMeta(ROUND_ID)).isEmpty();
    }

    @Test
    @DisplayName("v1 회차와 없는 회차는 게이트를 건드리지 않고 거절한다")
    void rejectsRoundsItMustNotTouch() {
        openGateWithStaleCounters();
        jdbc.update("UPDATE coupons SET issuance_engine_version = 'V1' WHERE id = ?", ROUND_ID);

        assertThat(runner().rebuild(ROUND_ID).status())
                .isEqualTo(CouponRoundRebuildStatus.ENGINE_NOT_V2);
        assertThat(gate.readMeta(ROUND_ID)).isPresent();
        assertThat(runner().rebuild(9_999).status())
                .isEqualTo(CouponRoundRebuildStatus.ROUND_NOT_FOUND);
    }

    @Test
    @DisplayName("재고 행이 없으면 게이트를 닫기 전에 거절한다")
    void rejectsMissingStockRow() {
        openGateWithStaleCounters();
        jdbc.update("DELETE FROM coupon_stocks WHERE coupon_id = ?", ROUND_ID);

        assertThat(runner().rebuild(ROUND_ID).status())
                .isEqualTo(CouponRoundRebuildStatus.STOCK_ROW_MISSING);
        assertThat(gate.readMeta(ROUND_ID)).isPresent();
    }

    @Test
    @DisplayName("게이트가 이미 닫힌 회차도 되살린다 — 키를 잃은 회차가 이 경로의 원래 대상이다")
    void rebuildsRoundWhoseKeysAreGone() {
        insertIssuance(1, 1, "ISSUED");

        CouponRoundRebuildResult result = runner().rebuild(ROUND_ID);

        assertThat(result.status()).isEqualTo(CouponRoundRebuildStatus.REBUILT);
        assertThat(gate.readMeta(ROUND_ID)).isPresent();
        assertThat(claim(600).outcome()).isEqualTo(ClaimOutcome.CLAIMED);
    }

    // ── 픽스처 ────────────────────────────────────────────────────────────────

    /** 시딩 한가운데 — 집계는 끝났고 {@code meta} 는 아직 없는 창 — 에서 무언가를 시킨다. */
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

    private com.kafkick.core.coupon.v2.port.ClaimResult claim(long memberId) {
        return gate.claim(new ClaimCommand(
                ROUND_ID, memberId, GRADE_BIT, "key-" + memberId, "token-" + memberId));
    }

    /** 게이트 한 곳만 갈아 끼우기 위한 위임. 테스트가 순서의 틈에 손을 넣는 자리다. */
    private static class DelegatingGate implements IssuanceGatePort {

        private final IssuanceGatePort delegate;

        DelegatingGate(IssuanceGatePort delegate) {
            this.delegate = delegate;
        }

        @Override
        public com.kafkick.core.coupon.v2.port.ClaimResult claim(ClaimCommand command) {
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
        public com.kafkick.core.coupon.v2.port.RestoreOutcome restore(
                long couponRoundId, long count) {
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
            delegate.writeMeta(couponRoundId, meta);
        }

        @Override
        public java.util.Optional<GateMeta> readMeta(long couponRoundId) {
            return delegate.readMeta(couponRoundId);
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private int activeCountColumn() {
        return jdbc.queryForObject(
                "SELECT active_count FROM coupon_stocks WHERE coupon_id = ?",
                Integer.class, ROUND_ID);
    }

    private static void insertTemplate() {
        jdbc.update("""
                INSERT INTO coupon_templates(
                    id, brand_id, name, policy_type, valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence, eligible_grades_mask,
                    active, created_at, updated_at)
                VALUES (?, ?, '템플릿', 'FIXED_AMOUNT', 30, 1, 'MON', '10:00:00', 1, ?, ?, true, ?, ?)
                """, TEMPLATE_ID, BRAND_ID, TOTAL_QUANTITY, GRADE_MASK, utc(NOW), utc(NOW));
    }

    private static void insertCoupon(long id, String engineVersion) {
        jdbc.update("""
                INSERT INTO coupons(
                    id, template_id, brand_id, name, policy_type, valid_days,
                    eligible_grades_mask, open_at, close_at, status, generated_at, created_at,
                    issuance_engine_version, issuance_engine_locked)
                VALUES (?, ?, ?, ?, 'FIXED_AMOUNT', 30, ?, ?, ?, 'OPEN', ?, ?, ?, true)
                """, id, TEMPLATE_ID, BRAND_ID, "회차 " + id, GRADE_MASK,
                utc(OPEN_AT), utc(CLOSE_AT), utc(OPEN_AT), utc(OPEN_AT), engineVersion);
    }

    private static void insertStock(long couponId, int totalQuantity, int activeCount) {
        jdbc.update("""
                INSERT INTO coupon_stocks(coupon_id, total_quantity, active_count, updated_at)
                VALUES (?, ?, ?, ?)
                """, couponId, totalQuantity, activeCount, utc(NOW));
    }

    private static void insertIssuance(long id, long memberId, String status) {
        jdbc.update("INSERT INTO members(id, membership_grade, created_at) VALUES (?, 'WELCOME', ?)",
                memberId, utc(OPEN_AT.minusSeconds(86_400)));
        jdbc.update("""
                INSERT INTO issuances(
                    id, coupon_id, member_id, code, issued_grade, status,
                    issued_at, expires_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'WELCOME', ?, ?, ?, ?, ?)
                """, id, ROUND_ID, memberId, String.format("C%015d", id), status,
                utc(OPEN_AT.plusSeconds(60)), utc(NOW.plusSeconds(86_400)),
                utc(OPEN_AT.plusSeconds(60)), utc(OPEN_AT.plusSeconds(60)));
    }

    private static LocalDateTime utc(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
