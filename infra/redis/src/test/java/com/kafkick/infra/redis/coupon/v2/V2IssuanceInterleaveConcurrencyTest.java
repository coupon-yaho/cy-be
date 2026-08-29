package com.kafkick.infra.redis.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.coupon.v2.IssuedValue;
import com.kafkick.core.coupon.v2.IssuedValueCodec;
import com.kafkick.core.coupon.v2.RequestTokenGenerator;
import com.kafkick.core.coupon.v2.V2CouponIssueException;
import com.kafkick.core.coupon.v2.V2CouponIssueService;
import com.kafkick.core.coupon.v2.V2CouponIssueResult;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.coupon.v2.port.CompleteOutcome;
import com.kafkick.core.coupon.v2.port.GateMeta;
import com.kafkick.core.coupon.v2.port.GateStatus;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.RebuiltIssued;
import com.kafkick.core.coupon.v2.port.ReclaimOutcome;
import com.kafkick.core.coupon.v2.port.RestorationHaltStore;
import com.kafkick.core.coupon.v2.port.RestoreOutcome;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.EngineVersion;

/**
 * C9 · C10 · C12 — 인터리브와 접두 충돌을 <b>호출부까지</b> 태워 본다.
 *
 * <p><b>래치 훅은 프로덕션 코드에 없다.</b> {@link V2CouponIssueService} 가 협력자를 전부
 * 생성자로 받으므로 08 이 지정한 두 지점이 그대로 협력자 경계 위에 얹힌다 —
 * ① INSERT 직전은 {@link IssuanceRepository#save}, ② 커밋 직후·완료 CAS 직전은
 * {@link IssuanceGatePort#complete} 다. 테스트 전용 데코레이터로 그 경계에서 멈춘다.
 * 프로덕션에 분기가 남으면 같은 코드를 재는 측정 하네스(CY-759)의 수치가 오염된다.
 *
 * <p>게이트는 <b>실물</b>이다. 목으로는 스크립트의 답을 테스트가 스스로 적게 되어
 * "완료 CAS 가 {@code -1} 을 낸다" 가 아무것도 증명하지 못한다.
 *
 * <p>{@code Thread.sleep} 을 쓰지 않는다. 두 스레드는 래치로만 만난다.
 *
 * <p><b>08 의 {@code completeCasMiss} 단언은 여기 없다</b> — 그 카운터는 S5 의 이상 카운터
 * 8종이고 10-작업분할이 측정 회차 전까지 미룬 것이라 저장소에 아직 없다. 계측이 붙으면
 * 완료 CAS 가 {@code -1} 인 두 테스트에 카운터 단언을 얹는다.
 */
@Testcontainers(disabledWithoutDocker = true)
class V2IssuanceInterleaveConcurrencyTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static final long ROUND_ID = 7;
    private static final long MEMBER_ID = 42;
    private static final int GRADE_MASK = 0b111;
    /** 헤드룸 0 이다 — 짝 없는 복원이 한 번만 통과해도 초과 발급이 확정된다. */
    private static final long TOTAL = 1;
    private static final long HOUR = 3_600_000L;
    private static final String IDEM = "123e4567-e89b-42d3-a456-426614174000";
    private static final Instant ISSUED_AT = Instant.parse("2026-08-29T00:00:00Z");
    private static final long TIMEOUT_SECONDS = 5;

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;

    private final IssuanceKeys keys = IssuanceKeys.of(ROUND_ID);
    private final IssuedValueCodec codec = new IssuedValueCodec();
    private final ExecutorService requestThread = Executors.newSingleThreadExecutor();

    private IssuanceGatePort gate;

    @BeforeAll
    static void startRedis() {
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        factory.afterPropertiesSet();
        factory.start();
        redis = new StringRedisTemplate(factory);
    }

    @AfterAll
    static void stopRedis() {
        factory.destroy();
    }

    @BeforeEach
    void reset() {
        gate = new RedisIssuanceGate(new IssuanceScriptRunner(redis), redis);
        redis.delete(List.of(keys.stock(), keys.issued(), keys.meta(), keys.issuedEver(),
                keys.restorationHalt()));
        redis.opsForValue().set(keys.stock(), Long.toString(TOTAL));
        redis.opsForValue().set(keys.issuedEver(), "0");
        long now = System.currentTimeMillis();
        gate.writeMeta(ROUND_ID, new GateMeta(
                GateStatus.OPEN, now - HOUR, now + HOUR, GRADE_MASK, TOTAL));
    }

    @AfterEach
    void stopRequestThread() {
        requestThread.shutdownNow();
    }

    /**
     * C9 — 회수와 커밋의 인터리브. 원 요청이 선점을 끝내고 INSERT 직전에 멈춰 있는 동안
     * 재구성의 파손 회수가 그 자리를 강제로 회수하고, 곧바로 원 요청이 커밋한다.
     *
     * <p>13 이 "발급이 도는 중에는 부르지 않는다" 로 금지한 호출을 <b>일부러</b> 만든다.
     * 지키는 것은 <b>살아 있는 선점은 회수가 건드리지 않는다</b> 하나다 — 그 판정이
     * 느슨해지면 짝 없는 {@code INCR} 이 그대로 초과 발급이다.
     */
    @Test
    @DisplayName("C9 · 커밋 직전의 살아 있는 선점은 회수가 건드리지 않는다 — 초과 발급 0")
    void reclaimDoesNotTouchALiveClaimBetweenClaimAndCommit() throws Exception {
        CountDownLatch atInsert = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        IssuanceRepository issuances = parkBeforeInsert(atInsert, resume);
        V2CouponIssueService service = service(gate, issuances);

        Future<V2CouponIssueResult> request =
                requestThread.submit(() -> service.issue(command(), definition()));
        assertThat(atInsert.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("① INSERT 직전에서 멈춰야 인터리브가 성립한다")
                .isTrue();

        // 선점은 이미 끝났고 커밋은 아직이다 — 이 창이 존재한다는 사실을 먼저 못박는다.
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo("0");
        String claimed = (String) redis.opsForHash().get(keys.issued(), memberField());
        assertThat(claimed).isNotNull();

        assertThat(gate.reclaimCorrupt(ROUND_ID, MEMBER_ID, true, TOTAL))
                .as("살아 있는 선점을 회수가 지우면 그것이 초과 발급 경로다")
                .isEqualTo(ReclaimOutcome.NOT_CORRUPT);
        assertThat(redis.opsForHash().get(keys.issued(), memberField())).isEqualTo(claimed);
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo("0");

        resume.countDown();

        assertThat(request.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).completeOutcome())
                .contains(CompleteOutcome.PROMOTED);
        // DB 발급 1건 · Redis 잔여 0 · 총재고 1. 셋이 맞물린다.
        org.mockito.Mockito.verify(issuances).save(any());
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo("0");
        assertThat(redis.opsForValue().get(keys.issuedEver())).isEqualTo("1");
    }

    /**
     * C9 의 뒷면 — 회수가 <b>실제로 물었을 때</b> 무엇이 남는지. 파손 값을 되돌린 직후
     * 원 요청이 커밋하면 완료 CAS 는 {@code -1} 이고, 그 자리에는 <b>DB 발급 1건과
     * 되살아난 재고 1장이 동시에</b> 남는다 — 다음 요청 하나가 그대로 초과 발급이다.
     *
     * <p>이 어긋남이 13 이 "게이트가 닫혀 있을 때만 회수한다" 로 금지한 이유다.
     * <b>여기서 안전을 단언하지 않는다</b> — 이 경로는 안전하지 않고, 그 사실을 박는 것이
     * 이 테스트다. 발급 경로가 회수를 부르게 되면(13 의 표가 금지) 이 단언이 깨져야 한다.
     */
    @Test
    @DisplayName("C9 뒷면 · 발급 중의 회수는 완료 CAS 를 -1 로 만들고 DB 와 재고를 어긋나게 남긴다")
    void reclaimDuringIssuanceLeavesTheStockAheadOfTheDatabase() throws Exception {
        CountDownLatch atInsert = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        IssuanceRepository issuances = parkBeforeInsert(atInsert, resume);
        V2CouponIssueService service = service(gate, issuances);

        Future<V2CouponIssueResult> request =
                requestThread.submit(() -> service.issue(command(), definition()));
        assertThat(atInsert.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        // 재구성이 파손 값을 남긴 자리라야 회수가 문다(13). 그 상태를 만들고 회수한다.
        redis.opsForHash().put(keys.issued(), memberField(), "P|1|broken");
        assertThat(gate.reclaimCorrupt(ROUND_ID, MEMBER_ID, true, TOTAL))
                .isEqualTo(ReclaimOutcome.RECLAIMED_AND_RESTORED);

        resume.countDown();

        assertThatThrownBy(() -> request.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .cause()
                .isInstanceOf(V2CouponIssueException.class)
                .hasMessageContaining("CLAIM_GONE");
        org.mockito.Mockito.verify(issuances).save(any());
        assertThat(redis.opsForValue().get(keys.stock()))
                .as("DB 에는 발급이 커밋됐는데 재고가 되살아나 있다 — 초과 발급 방향의 어긋남이다")
                .isEqualTo("1");
        assertThat(redis.<String, String>opsForHash().hasKey(keys.issued(), memberField()))
                .isFalse();
    }

    /**
     * C10 — 보상된 field 에 DONE 승격. 커밋은 끝났고 완료 CAS 직전에 멈춰 있는 동안
     * 보상이 그 선점을 되돌린다. 완료 CAS 가 <b>지워진 field 를 되살리면</b> 그 자리는
     * 아무도 못 쓰는 채 재고만 한 장 빠진다.
     */
    @Test
    @DisplayName("C10 · 보상 뒤의 완료 CAS 는 -1 이고 field 를 부활시키지 않는다")
    void completeAfterCompensationDoesNotResurrectTheField() throws Exception {
        CountDownLatch atComplete = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        IssuanceGatePort parked = parkBeforeComplete(gate, atComplete, resume);
        V2CouponIssueService service = service(parked, savingRepository());

        Future<?> request = requestThread.submit(() -> service.issue(command(), definition()));
        assertThat(atComplete.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("② 커밋 직후·완료 CAS 직전에서 멈춰야 인터리브가 성립한다")
                .isTrue();

        String token = storedToken();
        assertThat(gate.compensate(ROUND_ID, MEMBER_ID, token))
                .isEqualTo(CompensateOutcome.REVERTED);
        assertThat(redis.<String, String>opsForHash().hasKey(keys.issued(), memberField()))
                .as("보상이 field 를 지웠다 — 이 뒤의 완료 CAS 가 시험 대상이다")
                .isFalse();

        resume.countDown();

        assertThatThrownBy(() -> request.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .cause()
                .isInstanceOf(V2CouponIssueException.class)
                .hasMessageContaining("CLAIM_GONE");
        assertThat(redis.<String, String>opsForHash().hasKey(keys.issued(), memberField()))
                .as("field 가 부활하지 않는다")
                .isFalse();
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo(Long.toString(TOTAL));
        assertThat(redis.opsForValue().get(keys.issuedEver())).isEqualTo("0");
    }

    /**
     * C12 — 멱등키 접두 충돌. 같은 회원이 이미 {@code abcdef…} 로 선점한 자리에
     * {@code abc} 로 다시 들어온다. 접두 비교를 고친 목적은 <b>그 요청을 통과시키는 것이
     * 아니다</b> — 같은 회원이 이미 받았으면 거절이 맞다. 목적은 서로 다른 키를 같은 키로
     * 오인하지 않는 것뿐이라 답은 여전히 {@code -4} 다.
     *
     * <p>Lua 수준은 S2 가 닫았다. 여기서 보는 것은 <b>유스케이스가 그 {@code -4} 를 거절
     * 결과로 그대로 내보내고 DB 를 건드리지 않는가</b>와 <b>보상이 남의 선점을 못 건드리는가</b>다.
     */
    @Test
    @DisplayName("C12 · 접두가 겹치는 멱등키는 -4 로 거절되고 기존 선점과 재고가 그대로다")
    void prefixCollidingIdempotencyKeyIsRejectedWithoutTouchingTheClaim() {
        String storedKey = "abcdef-0123456789";
        String requestKey = "abc";
        IssuanceRepository issuances = savingRepository();
        V2CouponIssueService service = service(gate, issuances);

        V2CouponIssueResult first = service.issue(
                new CouponIssueCommand(
                        ROUND_ID, MEMBER_ID, MembershipGrade.GOLD, storedKey, ISSUED_AT),
                definition());
        assertThat(first.claimResult().outcome()).isEqualTo(ClaimOutcome.CLAIMED);
        String storedValue = (String) redis.opsForHash().get(keys.issued(), memberField());

        V2CouponIssueResult collided = service.issue(
                new CouponIssueCommand(
                        ROUND_ID, MEMBER_ID, MembershipGrade.GOLD, requestKey, ISSUED_AT),
                definition());

        assertThat(collided.claimResult().outcome())
                .as("접두가 겹쳐도 같은 키가 아니다 — 재시도(-6/-7)가 아니라 중복(-4)이다")
                .isEqualTo(ClaimOutcome.DUP_PER_MEMBER);
        assertThat(collided.issueResult()).isEmpty();
        // 첫 발급의 INSERT 한 번뿐이다. 거절이 DB 를 건드리면 여기서 깨진다.
        org.mockito.Mockito.verify(issuances, org.mockito.Mockito.times(1)).save(any());
        assertThat(redis.opsForHash().get(keys.issued(), memberField()))
                .as("기존 선점 불변")
                .isEqualTo(storedValue);
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo("0");

        // 보상은 토큰 CAS 다. 충돌한 요청의 토큰으로는 남의 선점을 못 되돌린다.
        assertThat(gate.compensate(ROUND_ID, MEMBER_ID, "api-s9-other-1"))
                .isEqualTo(CompensateOutcome.NOT_MINE);
        assertThat(redis.opsForHash().get(keys.issued(), memberField())).isEqualTo(storedValue);
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo("0");
    }

    /**
     * ⑤ 복원끼리의 경합. 헤드룸이 1인데 두 스레드가 동시에 1건씩 되돌리려 한다.
     * <b>정확히 하나만</b> 통과해야 한다 — 둘 다 통과하면 그 순간 재고가 총재고를 넘고
     * 초과 발급이 확정된다.
     *
     * <p><b>이 테스트가 순차 실행보다 더 잡는 것은 없다</b> — Lua 는 서버에서 직렬화되므로
     * 관측 가능한 결과 공간이 같다. 여기서 박는 것은 "여러 클라이언트가 같은 키를 동시에
     * 두드려도 상한이 결과 집합을 좁힌다" 는 사실이고, 진짜 원자성 회귀(검사를 스크립트
     * 밖으로 빼는 것)는 순차 테스트도 함께 잡는다.
     */
    @Test
    @DisplayName("헤드룸 1에 동시 복원 2건 — 정확히 하나만 RESTORED 이고 재고는 총재고를 안 넘는다")
    void concurrentRestoresCannotBothPassTheCap() throws Exception {
        // 한 장이 나간 상태 = 헤드룸 1. 두 요청이 그 한 자리를 두고 겹친다.
        redis.opsForValue().set(keys.stock(), "0");
        int threads = 2;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch fire = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<RestoreOutcome>> results = new java.util.ArrayList<>();
            for (int index = 0; index < threads; index++) {
                results.add(pool.submit(() -> {
                    ready.countDown();
                    awaitFire(fire);
                    return gate.restore(ROUND_ID, 1);
                }));
            }
            assertThat(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            fire.countDown();

            List<RestoreOutcome> outcomes = new java.util.ArrayList<>();
            for (Future<RestoreOutcome> result : results) {
                outcomes.add(result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
            assertThat(outcomes)
                    .as("둘 다 통과하면 그 자리에서 초과 발급이다")
                    .containsExactlyInAnyOrder(RestoreOutcome.RESTORED, RestoreOutcome.OVER_CAP);
            assertThat(Long.parseLong(redis.opsForValue().get(keys.stock())))
                    .isEqualTo(TOTAL);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * ⑤ 표식과 재구성의 교차. 재구성이 표식을 푼 <b>직후</b> 뒤늦은 {@code -2} 가 도착하면
     * 표식이 되살아난다 — 재고는 정상인데 그 회차 만료만 멈춘 채 남는다.
     *
     * <p><b>이 테스트는 안전을 단언하지 않는다.</b> 창이 열려 있다는 사실 자체를 박는다.
     * 닫는 것은 코드가 아니라 절차다 — 재구성은 게이트를 닫고 진행 중인 복원이 빠진 뒤에
     * 시딩해야 한다(07). 그 계약이 지켜지지 않으면 여기 단언한 상태가 실제로 나온다.
     */
    @Test
    @DisplayName("재구성이 푼 표식은 뒤늦은 -2 로 되살아난다 — 절차가 닫아야 하는 창")
    void aLateHaltCanOutliveReconstruction() {
        RestorationHaltStore haltStore = new RedisRestorationHaltStore(redis);
        RedisIssuanceWarmup warmup = new RedisIssuanceWarmup(redis, haltStore);
        haltStore.halt(ROUND_ID);

        warmup.seedCounters(ROUND_ID, List.of(new RebuiltIssued(1, 1L)), TOTAL);
        assertThat(haltStore.isHalted(ROUND_ID))
                .as("시딩이 표식을 푼다")
                .isFalse();

        // 시딩 전에 -2 를 받아 둔 스레드가 이제서야 표식을 남긴다.
        haltStore.halt(ROUND_ID);

        assertThat(haltStore.isHalted(ROUND_ID))
                .as("재고는 정상인데 표식만 남는다. 게이트를 닫고 드레인하는 절차가 이 창을 닫는다")
                .isTrue();
    }

    /**
     * 만료 배치와 사용자 취소가 <b>같은 회차에 동시에</b> 복원을 쏜다. 취소는 멈추지 않기로
     * 했으므로(06) 이 겹침은 운영에서 늘 일어난다. 상한이 두 경로 어느 쪽으로도 안 새는지를
     * 본다 — 재고가 총재고를 넘는 순간 남은 자리는 그대로 초과 발급이다.
     */
    @Test
    @DisplayName("만료 배치와 취소가 동시에 복원해도 재고는 총재고를 넘지 않는다")
    void batchAndCancelRestoresNeverCrossTheCap() throws Exception {
        long total = 10;
        long alreadyBack = 4;
        // 6장이 나가 있고 4장이 남았다 — 헤드룸은 6이다. 배치와 취소가 각각 4건을 동시에
        // 되돌리면 합이 8이라 헤드룸을 넘으므로 한쪽은 반드시 거절돼야 한다.
        long now = System.currentTimeMillis();
        gate.writeMeta(ROUND_ID, new GateMeta(
                GateStatus.OPEN, now - HOUR, now + HOUR, GRADE_MASK, total));
        redis.opsForValue().set(keys.stock(), Long.toString(alreadyBack));

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch fire = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<RestoreOutcome> batch = pool.submit(() -> {
                ready.countDown();
                awaitFire(fire);
                return gate.restore(ROUND_ID, 4);
            });
            Future<RestoreOutcome> cancel = pool.submit(() -> {
                ready.countDown();
                awaitFire(fire);
                return gate.restore(ROUND_ID, 4);
            });
            assertThat(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            fire.countDown();

            List<RestoreOutcome> outcomes = List.of(
                    batch.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    cancel.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertThat(outcomes)
                    .as("헤드룸 6에 4+4 다 — 하나는 반드시 거절이다")
                    .containsExactlyInAnyOrder(RestoreOutcome.RESTORED, RestoreOutcome.OVER_CAP);
            assertThat(Long.parseLong(redis.opsForValue().get(keys.stock())))
                    .as("한 쪽만 통과한다 — 상한 검사를 지우면 12가 되어 깨진다")
                    .isEqualTo(alreadyBack + 4);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * 래치가 안 풀린 채 흘러가면 두 스레드가 <b>순차로</b> 쏴도 단언이 통과한다 —
     * "동시에 쐈다" 를 증명하지 못한 채 초록이 된다. 반환값을 반드시 본다.
     */
    private static void awaitFire(CountDownLatch fire) throws InterruptedException {
        if (!fire.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException("래치가 풀리지 않았다");
        }
    }

    // --- 래치 데코레이터 — 테스트 소스셋에만 있다 ------------------------------------

    /** ① INSERT 직전. {@code transactions.execute} 안이므로 트랜잭션 경계 안에서 멈춘다. */
    private static IssuanceRepository parkBeforeInsert(CountDownLatch arrived, CountDownLatch resume) {
        IssuanceRepository repository = mock(IssuanceRepository.class);
        when(repository.save(any())).thenAnswer(invocation -> {
            arrived.countDown();
            if (!resume.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("래치가 풀리지 않았다");
            }
            return persisted(invocation.getArgument(0));
        });
        return repository;
    }

    /** ② 커밋 직후·완료 CAS 직전. 위임 전에 멈춘다. */
    private static IssuanceGatePort parkBeforeComplete(
            IssuanceGatePort delegate, CountDownLatch arrived, CountDownLatch resume) {
        return new DelegatingGate(delegate) {
            @Override
            public com.kafkick.core.coupon.v2.port.CompleteOutcome complete(
                    long couponRoundId, long memberId, String requestToken) {
                arrived.countDown();
                try {
                    if (!resume.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("래치가 풀리지 않았다");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return super.complete(couponRoundId, memberId, requestToken);
            }
        };
    }

    private static class DelegatingGate implements IssuanceGatePort {

        private final IssuanceGatePort delegate;

        DelegatingGate(IssuanceGatePort delegate) {
            this.delegate = delegate;
        }

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
        public CompensateOutcome compensate(
                long couponRoundId, long memberId, String requestToken) {
            return delegate.compensate(couponRoundId, memberId, requestToken);
        }

        @Override
        public com.kafkick.core.coupon.v2.port.RestoreOutcome restore(
                long couponRoundId, long count) {
            return delegate.restore(couponRoundId, count);
        }

        @Override
        public ReclaimOutcome reclaimCorrupt(
                long couponRoundId, long memberId, boolean restoreStock, long totalQuantity) {
            return delegate.reclaimCorrupt(couponRoundId, memberId, restoreStock, totalQuantity);
        }

        @Override
        public void writeMeta(long couponRoundId, GateMeta meta) {
            delegate.writeMeta(couponRoundId, meta);
        }

        @Override
        public Optional<GateMeta> readMeta(long couponRoundId) {
            return delegate.readMeta(couponRoundId);
        }
    }

    // --- 조립 -----------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static V2CouponIssueService service(IssuanceGatePort gate, IssuanceRepository issuances) {
        IdempotencyRepository idempotencies = mock(IdempotencyRepository.class);
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any()))
                .thenReturn(true);
        // 접두 비교가 깨져 -6 으로 오인되면 replayDone 이 이 조회를 부른다. 비워 두면 NPE 로
        // 죽어 "목 설정 실수" 로 오진하므로, 빈 값을 돌려 그 줄에서 뜻이 드러나게 한다.
        when(idempotencies.findByKey(any())).thenReturn(Optional.empty());
        IdempotencyResultCodec<CouponIssueResult> resultCodec = mock(IdempotencyResultCodec.class);
        when(resultCodec.write(any())).thenReturn("{result}");
        return new V2CouponIssueService(
                gate,
                issuances,
                mock(IssuanceHistoryRepository.class),
                idempotencies,
                mock(CouponStockRepository.class),
                () -> "1234567890ABCDEF",
                resultCodec,
                new RequestTokenGenerator("api-s9"),
                immediateTransactions());
    }

    private static IssuanceRepository savingRepository() {
        IssuanceRepository repository = mock(IssuanceRepository.class);
        when(repository.save(any()))
                .thenAnswer(invocation -> persisted(invocation.getArgument(0)));
        return repository;
    }

    private static Issuance persisted(Issuance issuance) {
        return Issuance.restore(99L, issuance.couponRoundId(), issuance.memberId(), issuance.code(),
                issuance.issuedGrade(), issuance.status(), issuance.issuedAt(),
                issuance.expiresAt(), issuance.updatedAt());
    }

    @SuppressWarnings("unchecked")
    private static TransactionOperations immediateTransactions() {
        TransactionOperations operations = mock(TransactionOperations.class);
        when(operations.execute(any())).thenAnswer(invocation ->
                ((TransactionCallback<Object>) invocation.getArgument(0))
                        .doInTransaction(mock(TransactionStatus.class)));
        return operations;
    }

    private static CouponIssueCommand command() {
        return new CouponIssueCommand(ROUND_ID, MEMBER_ID, MembershipGrade.GOLD, IDEM, ISSUED_AT);
    }

    private static CouponRoundIssuanceDefinition definition() {
        return new CouponRoundIssuanceDefinition(ROUND_ID, 7, EngineVersion.V2);
    }

    private static String memberField() {
        return Long.toString(MEMBER_ID);
    }

    private String storedToken() {
        String stored = (String) redis.opsForHash().get(keys.issued(), memberField());
        IssuedValue value = codec.decode(stored);
        return value.requestToken();
    }
}
