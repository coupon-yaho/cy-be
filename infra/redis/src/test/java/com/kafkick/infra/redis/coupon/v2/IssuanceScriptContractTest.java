package com.kafkick.infra.redis.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.coupon.v2.IssuedValue;
import com.kafkick.core.coupon.v2.IssuedValueCodec;
import com.kafkick.core.coupon.v2.IssuedValueCorruptException;

/**
 * Lua 계약은 <b>실제 Redis 에서만</b> 검증된다. 목으로 세우면 스크립트가 무엇을 돌려주는지를
 * 테스트가 정하게 되어, 스크립트를 어떻게 고쳐도 통과한다.
 *
 * <p>거절 분기도 전부 실행한다. 코드가 몇 번인지가 아니라 <b>거절이 재고·issued_ever·field 를
 * 어떤 상태로 남기는지</b>가 계약이다 — 매진 롤백을 지워도 초록인 테이블은 아무것도 안 지킨다.
 *
 * <p>교차 판정 테이블은 Java codec 과 Lua 가 <b>같은 입력에 같은 답</b>을 내는지를 본다.
 * 한쪽만 통과하는 입력이 남으면 {@code LUA_GAP} 이 0 으로 수렴하지 못한다(08 문서).
 */
@Testcontainers(disabledWithoutDocker = true)
class IssuanceScriptContractTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static final String STOCK = "cy:v2:stock:{r}";
    private static final String ISSUED = "cy:v2:issued:{r}";
    private static final String META = "cy:v2:meta:{r}";
    private static final String ISSUED_EVER = "cy:v2:issued_ever:{r}";

    private static final String MEMBER = "42";
    private static final long HOUR = 3_600_000L;

    /** 게이트 창과 선점시각 검증의 기준. 시각의 원본이 Redis 라 테스트도 실제 시간을 쓴다. */
    private long testNow;
    private static final String GRADE_BIT = "1";
    private static final String TOKEN_A = "api-1-n1-t1-1";
    private static final String TOKEN_B = "api-2-n7-t3-9";
    private static final int TOTAL = 10;

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;

    private final IssuedValueCodec codec = new IssuedValueCodec();

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
    void resetKeys() {
        redis.delete(List.of(STOCK, ISSUED, META, ISSUED_EVER));
        redis.opsForValue().set(STOCK, Integer.toString(TOTAL));
        redis.opsForValue().set(ISSUED_EVER, "0");
        testNow = System.currentTimeMillis();
        givenOpenGate();
    }

    // ---------- 선점 성공과 재시도 ----------

    @Test
    @DisplayName("선점은 재고를 깎고 issued_ever 를 올리고 잔여 재고를 돌려준다")
    void claimWritesAllThreeInOneScript() {
        List<?> result = claimResult("key-1", TOKEN_A);

        assertThat(code(result)).isEqualTo(IssuanceScriptCodes.Claim.OK);
        assertThat(((Number) result.get(1)).longValue())
                .as("둘째 원소가 잔여 재고다 — 호출부가 이걸 응답에 쓴다")
                .isEqualTo(TOTAL - 1);
        assertThat(stock()).isEqualTo(TOTAL - 1);
        assertThat(issuedEver()).isEqualTo(1);
        assertThat(stored())
                .as("Lua 가 만든 값과 Java 가 만드는 값은 글자 하나까지 같아야 한다 — "
                        + "여기서 5번째 필드가 붙어도 codec 은 그걸 멱등키로 흡수한다")
                .isEqualTo(codec.encode(new IssuedValue(
                        IssuedValue.Status.PENDING, claimedAt(), TOKEN_A, "key-1")));
        assertThat(codec.decode(stored()).hasIdempotencyKey("key-1")).isTrue();
        assertThat(claimedAt())
                .as("선점시각은 호출자가 아니라 Redis 가 정한다 — 인스턴스 시계가 섞이면 안 된다")
                .isBetween(testNow - 5_000, testNow + 5_000);
    }

    @Test
    @DisplayName("선점 → 완료 → 같은 키 재시도 = -6")
    void retryAfterCompleteIsReplayDone() {
        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.OK);
        assertThat(complete(TOKEN_A)).isEqualTo(IssuanceScriptCodes.Complete.PROMOTED);

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.REPLAY_DONE);
        assertThat(stock()).as("재시도는 재고를 또 깎지 않는다").isEqualTo(TOTAL - 1);
    }

    @Test
    @DisplayName("선점 → 보상 → 재시도 = 새 선점 성공. field 가 지워졌다")
    void retryAfterCompensateClaimsAgain() {
        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.OK);
        assertThat(compensate(TOKEN_A)).isEqualTo(IssuanceScriptCodes.Compensate.REVERTED);
        assertThat(stock()).isEqualTo(TOTAL);
        assertThat(issuedEver()).isEqualTo(0);

        assertThat(claim("key-1", TOKEN_B)).isEqualTo(IssuanceScriptCodes.Claim.OK);
        assertThat(stock()).isEqualTo(TOTAL - 1);
    }

    @Test
    @DisplayName("승격은 상태 한 글자만 바꾼다 — 선점시각·토큰·멱등키가 전부 보존된다")
    void completeKeepsEverythingButStatus() {
        claim("key-1", TOKEN_A);
        String beforePromotion = stored();

        assertThat(complete(TOKEN_A)).isEqualTo(IssuanceScriptCodes.Complete.PROMOTED);
        assertThat(stored())
                .as("완료시각의 원본은 DB 다. Redis 에만 있는 선점시각을 덮으면 그 정보가 사라진다")
                .isEqualTo("D" + beforePromotion.substring(1));
        assertThat(codec.decode(stored()).claimedAtEpochMillis())
                .as("claimedAtEpochMillis 라는 이름이 D 상태에서도 참이어야 한다")
                .isBetween(testNow - 5_000, testNow + 5_000);
        assertThat(complete(TOKEN_A))
                .as("재시도끼리 겹친 것이라 정상이다")
                .isEqualTo(IssuanceScriptCodes.Complete.ALREADY_DONE);
    }

    // ---------- 토큰이 방어선이다 ----------

    @Test
    @DisplayName("A 의 선점을 B 의 토큰으로 보상하면 0 이고 상태가 그대로다")
    void compensateWithForeignTokenDoesNothing() {
        claim("key-1", TOKEN_A);
        String before = stored();

        assertThat(compensate(TOKEN_B)).isEqualTo(IssuanceScriptCodes.Compensate.NOT_MINE);
        assertThat(stored()).isEqualTo(before);
        assertThat(stock()).isEqualTo(TOTAL - 1);
        assertThat(issuedEver()).isEqualTo(1);
    }

    @Test
    @DisplayName("A 의 선점을 B 의 토큰으로 완료하면 -2 이고 상태가 그대로다")
    void completeWithForeignTokenIsRejected() {
        claim("key-1", TOKEN_A);
        String before = stored();

        assertThat(complete(TOKEN_B)).isEqualTo(IssuanceScriptCodes.Complete.FOREIGN_CLAIM);
        assertThat(stored()).isEqualTo(before);
    }

    @Test
    @DisplayName("완료된 건에 보상하면 -1 이고 field 가 부활하지 않는다")
    void compensateOnDoneIsRejected() {
        claim("key-1", TOKEN_A);
        complete(TOKEN_A);

        assertThat(compensate(TOKEN_A)).isEqualTo(IssuanceScriptCodes.Compensate.ALREADY_DONE);
        assertThat(stock()).isEqualTo(TOTAL - 1);
        assertThat(issuedEver()).isEqualTo(1);
    }

    @Test
    @DisplayName("보상된 자리에 완료를 걸면 -1 이고 field 가 되살아나지 않는다")
    void completeAfterCompensateDoesNotResurrect() {
        claim("key-1", TOKEN_A);
        compensate(TOKEN_A);

        assertThat(complete(TOKEN_A)).isEqualTo(IssuanceScriptCodes.Complete.CLAIM_GONE);
        assertThat(stored()).isNull();
    }

    @Test
    @DisplayName("접두가 겹치는 키는 다른 키다 — 발급 -4, 보상은 남의 선점을 안 건드린다")
    void prefixCollisionIsNotTheSameKey() {
        claim("abcdef", TOKEN_A);
        String before = stored();

        assertThat(claim("abc", TOKEN_B)).isEqualTo(IssuanceScriptCodes.Claim.DUP_PER_MEMBER);
        assertThat(compensate(TOKEN_B)).isEqualTo(IssuanceScriptCodes.Compensate.NOT_MINE);
        assertThat(stored()).isEqualTo(before);
        assertThat(stock()).isEqualTo(TOTAL - 1);
    }

    @Test
    @DisplayName("같은 키의 아직 처리 중인 재시도는 -7 이다")
    void replayOfPendingClaim() {
        claim("key-1", TOKEN_A);

        assertThat(claim("key-1", TOKEN_B)).isEqualTo(IssuanceScriptCodes.Claim.REPLAY_PENDING);
    }

    // ---------- 거절 분기 ----------

    @Test
    @DisplayName("매진이면 -5 이고 방금 잡은 선점만 되돌린다")
    void soldOutRollsBackOnlyItsOwnClaim() {
        redis.opsForValue().set(STOCK, "0");

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.SOLD_OUT);
        assertThat(stored()).as("HDEL 이 빠지면 이 회원은 영영 -4 로 막힌다").isNull();
        assertThat(stock()).isEqualTo(0);
        assertThat(issuedEver()).isEqualTo(0);
    }

    @Test
    @DisplayName("stock 키가 없으면 매진이 아니라 -11 이고, 방금 만든 선점을 되돌린다")
    void missingStockIsNotSoldOut() {
        redis.delete(STOCK);

        assertThat(claim("key-1", TOKEN_A))
                .as("재고가 남았는데 전량 매진으로 종단 거절되면 안 된다")
                .isEqualTo(IssuanceScriptCodes.Claim.COUNTER_UNREADABLE);
        assertThat(stored()).as("HSETNX 로 방금 만든 field 는 되돌린다").isNull();
        assertThat(issuedEver()).isEqualTo(0);
    }

    @Test
    @DisplayName("stock 값이 숫자가 아니어도 -11 이다")
    void nonNumericStockIsStockMissing() {
        redis.opsForValue().set(STOCK, "nope");

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.COUNTER_UNREADABLE);
        assertThat(stored()).isNull();
    }

    @Test
    @DisplayName("stock 이 엉뚱한 자료형이어도 -11 이고 고아 field 를 남기지 않는다")
    void wrongTypeStockLeavesNoOrphanClaim() {
        redis.delete(STOCK);
        redis.opsForHash().put(STOCK, "wrong", "type");

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.COUNTER_UNREADABLE);
        assertThat(stored())
                .as("HSETNX 뒤에서 터지면 DECR 없는 P 가 남는다 — 형식은 멀쩡해 회수도 못 한다")
                .isNull();
        assertThat(issuedEver()).isEqualTo(0);
    }

    @Test
    @DisplayName("복원도 stock 자료형 오류를 -11 로 본다 — 예외로 터지면 S6 이 장애로 오인해 삼킨다")
    void wrongTypeStockOnRestoreIsStockMissing() {
        redis.delete(STOCK);
        redis.opsForHash().put(STOCK, "wrong", "type");

        assertThat(restore("1")).isEqualTo(IssuanceScriptCodes.Restore.STOCK_MISSING);
    }

    @ParameterizedTest(name = "[{index}] stock = {0}")
    @DisplayName("stock 이 비정수면 -11 이다 — 통과시키면 DECR 이 터지고 고아 P 가 남는다")
    @ValueSource(strings = {"10.5", "1e1", " 10 ", "007", "-0", "+7", "0009"})
    void nonIntegerStockIsStockMissing(String stock) {
        redis.opsForValue().set(STOCK, stock);

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.COUNTER_UNREADABLE);
        assertThat(stored())
                .as("Lua 는 원자적이어도 이미 적용된 쓰기를 되돌리지 않는다")
                .isNull();
        assertThat(redis.opsForValue().get(STOCK)).isEqualTo(stock);
    }

    @ParameterizedTest(name = "[{index}] issued_ever = {0}")
    @DisplayName("issued_ever 가 비정수면 stock 을 깎기 전에 멈춘다 — 축 하나만 어긋나는 상태가 없다")
    @ValueSource(strings = {"notanumber", "3.5", "007", "-0", "+7"})
    void nonIntegerIssuedEverStopsBeforeAnyWrite(String ever) {
        redis.opsForValue().set(ISSUED_EVER, ever);

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.COUNTER_UNREADABLE);
        assertThat(stock()).as("DECR 만 성공하고 INCR 이 터지면 PERSIST_GAP 이 깨진다").isEqualTo(TOTAL);
        assertThat(stored()).isNull();
        assertThat(redis.opsForValue().get(ISSUED_EVER)).isEqualTo(ever);
    }

    @ParameterizedTest(name = "[{index}] stock = {0}")
    @DisplayName("int64 를 넘는 카운터도 -11 이다 — DECR 이 out of range 로 거부한다")
    @ValueSource(strings = {"9007199254740993", "9999999999999999999", "99999999999999999999999"})
    void outOfRangeCounterIsUnreadable(String huge) {
        redis.opsForValue().set(STOCK, huge);

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.COUNTER_UNREADABLE);
        assertThat(stored()).isNull();

        redis.opsForValue().set(STOCK, Integer.toString(TOTAL));
        redis.opsForValue().set(ISSUED_EVER, huge);
        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.COUNTER_UNREADABLE);
        assertThat(stock()).isEqualTo(TOTAL);
    }

    @Test
    @DisplayName("15자리는 정상 범위다 — 상한을 너무 좁게 잡는 실수를 잡는다")
    void fifteenDigitCounterIsAccepted() {
        redis.opsForValue().set(ISSUED_EVER, "100000000000000");

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.OK);
        assertThat(issuedEver()).isEqualTo(100_000_000_000_001L);
    }

    @Test
    @DisplayName("허용 집합은 INCR 에 대해 닫혀 있어야 한다 — 자기가 읽을 수 없는 값을 쓰지 않는다")
    void claimRefusesWhenIncrementWouldLeaveTheReadableSet() {
        redis.opsForValue().set(ISSUED_EVER, "999999999999999");

        assertThat(claim("key-1", TOKEN_A))
                .as("INCR 하면 16자리가 되고 그다음 호출부터 우리 가드가 -11 로 막는다")
                .isEqualTo(IssuanceScriptCodes.Claim.COUNTER_UNREADABLE);
        assertThat(redis.opsForValue().get(ISSUED_EVER)).isEqualTo("999999999999999");
        assertThat(stored()).isNull();
        assertThat(stock()).isEqualTo(TOTAL);
    }

    @Test
    @DisplayName("경계에서 두 번 연속 선점해도 카운터가 계속 읽힌다")
    void consecutiveClaimsStayReadableAtTheBoundary() {
        redis.opsForValue().set(ISSUED_EVER, "99999999999998");

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.OK);
        List<?> second = redis.execute(IssuanceScripts.CLAIM,
                List.of(STOCK, ISSUED, META, ISSUED_EVER),
                "other-member", GRADE_BIT, "key-2", TOKEN_B);

        assertThat(code(second)).isEqualTo(IssuanceScriptCodes.Claim.OK);
        assertThat(issuedEver()).isEqualTo(100_000_000_000_000L);
    }

    @Test
    @DisplayName("정밀도 밖 총재고는 상한 검사를 무력화하므로 복원이 -1 이다")
    void totalBeyondDoublePrecisionIsNotReady() {
        // Lua 5.1 의 수는 double 이라 2^53 위에서는 a + 1 > a 가 false 다.
        // 그 값을 상한으로 쓰면 left + n > total 이 조용히 통과한다.
        redis.opsForHash().put(META, "totalQuantity", "1000000000000000001");

        assertThat(restore("1")).isEqualTo(IssuanceScriptCodes.Restore.NOT_READY);
    }

    @Test
    @DisplayName("issued_ever 키가 아직 없는 것은 정상이다 — 예열 구간을 막지 않는다")
    void absentIssuedEverIsAllowed() {
        redis.delete(ISSUED_EVER);

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.OK);
        assertThat(issuedEver()).isEqualTo(1);
    }

    @Test
    @DisplayName("보상도 비정수 카운터면 아무것도 안 바꾼다")
    void compensateStopsOnUnreadableCounters() {
        claim("key-1", TOKEN_A);
        String before = stored();
        redis.opsForValue().set(ISSUED_EVER, "notanumber");

        assertThat(compensate(TOKEN_A)).isEqualTo(IssuanceScriptCodes.Compensate.COUNTER_UNREADABLE);
        assertThat(stored()).as("HDEL 과 INCR 만 적용된 채로 끝나면 안 된다").isEqualTo(before);
        assertThat(stock()).isEqualTo(TOTAL - 1);
    }

    @Test
    @DisplayName("회수도 비정수 카운터면 아무것도 안 바꾼다")
    void reclaimStopsOnUnreadableCounters() {
        claim("key-1", TOKEN_A);
        redis.opsForHash().put(ISSUED, MEMBER, "X|1|t|k");
        redis.opsForValue().set(ISSUED_EVER, "notanumber");

        assertThat(reclaim(true)).isEqualTo(IssuanceScriptCodes.Reclaim.COUNTER_UNREADABLE);
        assertThat(stored()).isEqualTo("X|1|t|k");
        assertThat(stock()).isEqualTo(TOTAL - 1);
    }

    @Test
    @DisplayName("회수의 총재고 인자도 canonical 정수여야 한다")
    void reclaimRejectsNonCanonicalTotal() {
        redis.opsForHash().put(ISSUED, MEMBER, "X|1|t|k");

        assertThat(redis.execute(IssuanceScripts.RECLAIM_CORRUPT,
                List.of(STOCK, ISSUED, ISSUED_EVER), MEMBER, "1", "007"))
                .isEqualTo(IssuanceScriptCodes.Reclaim.BAD_ARGUMENT);
        assertThat(stored()).isEqualTo("X|1|t|k");
    }

    @Test
    @DisplayName("복원은 비정수 stock 을 상한 초과(-2)로 오진하지 않는다")
    void restoreDoesNotMistakeUnreadableStockForOverCap() {
        redis.opsForValue().set(STOCK, "10.5");

        assertThat(restore("1"))
                .as("-2 는 그 회차 만료를 멈추고 경보하라는 뜻이라 원인이 뒤바뀐다")
                .isEqualTo(IssuanceScriptCodes.Restore.STOCK_MISSING);
    }

    @Test
    @DisplayName("매진 거절은 이미 있던 남의 선점을 지우지 않는다")
    void soldOutDoesNotTouchAnExistingClaim() {
        claim("key-1", TOKEN_A);
        redis.opsForValue().set(STOCK, "0");
        String before = stored();

        assertThat(claim("key-2", TOKEN_B)).isEqualTo(IssuanceScriptCodes.Claim.DUP_PER_MEMBER);
        assertThat(stored()).isEqualTo(before);
    }

    @Test
    @DisplayName("마감·미오픈·등급 미달은 각각 -1 · -2 · -3 이고 아무것도 안 쓴다")
    void gateRejectionsWriteNothing() {
        givenGate("CLOSED", testNow - HOUR, testNow + HOUR, "7");
        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.CLOSED);

        givenGate("OPEN", testNow - HOUR, testNow - 1000, "7");
        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.CLOSED);

        givenGate("OPEN", testNow + HOUR, testNow + 2 * HOUR, "7");
        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.NOT_OPEN);

        givenGate("OPEN", testNow - HOUR, testNow + HOUR, "6");
        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.GRADE_NOT_ALLOWED);

        assertThat(stored()).isNull();
        assertThat(stock()).isEqualTo(TOTAL);
        assertThat(issuedEver()).isEqualTo(0);
    }

    @Test
    @DisplayName("meta 가 없으면 -9 다")
    void missingGateIsNotReady() {
        redis.delete(META);

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.NOT_READY);
    }

    @ParameterizedTest(name = "[{index}] 빠진 필드 = {0}")
    @DisplayName("meta 가 부분 상태여도 런타임 에러가 아니라 -9 다 — 재구성 창의 요청은 503 이어야 한다")
    @ValueSource(strings = {"openAt", "closeAt", "gradeMask", "totalQuantity"})
    void partialGateIsNotReady(String missingField) {
        redis.opsForHash().delete(META, missingField);

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.NOT_READY);
    }

    @Test
    @DisplayName("게이트가 열렸는데 복원만 죽는 상태가 없다 — 두 스크립트가 같은 meta 계약을 본다")
    void gateAndRestoreRequireTheSameMetaFields() {
        redis.opsForHash().delete(META, "totalQuantity");

        assertThat(claim("key-1", TOKEN_A))
                .as("발급은 되는데 만료 복원만 영구히 죽는 조합이 만들어지면 안 된다")
                .isEqualTo(IssuanceScriptCodes.Claim.NOT_READY);
        assertThat(restore("1")).isEqualTo(IssuanceScriptCodes.Restore.NOT_READY);
    }

    @Test
    @DisplayName("status 가 빈 문자열이면 마감(-1) 이 아니라 미준비(-9) 다")
    void emptyStatusIsNotReady() {
        redis.opsForHash().put(META, "status", "");

        assertThat(claim("key-1", TOKEN_A))
                .as("Lua 에서 '' 는 truthy 라 존재 검사를 그냥 통과한다")
                .isEqualTo(IssuanceScriptCodes.Claim.NOT_READY);
    }

    @ParameterizedTest(name = "[{index}] gradeMask = {0}")
    @DisplayName("meta 의 등급 마스크도 범위를 본다 — bit.band(-1, x) 는 전 등급 통과다")
    @ValueSource(strings = {"-1", "4294967296", "1.5"})
    void outOfRangeGradeMaskIsNotReady(String mask) {
        givenGate("OPEN", testNow - HOUR, testNow + HOUR, mask);

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.NOT_READY);
        assertThat(stock()).isEqualTo(TOTAL);
    }

    @Test
    @DisplayName("meta 값이 숫자가 아니어도 -9 다")
    void nonNumericGateIsNotReady() {
        redis.opsForHash().put(META, "closeAt", "nope");

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.NOT_READY);
    }

    // ---------- 인자 가드 ----------

    @ParameterizedTest(name = "[{index}] gradeBit = {0}")
    @DisplayName("등급 비트가 숫자가 아니거나 정수가 아니면 -10 이다 — bit.band 가 터지면 정체불명 5xx 다")
    @ValueSource(strings = {"nope", "1.5", "", "-1"})
    void rejectsNonIntegerGradeBit(String gradeBit) {
        List<?> result = redis.execute(IssuanceScripts.CLAIM,
                List.of(STOCK, ISSUED, META, ISSUED_EVER),
                MEMBER, gradeBit, "key-1", TOKEN_A);

        assertThat(code(result)).isEqualTo(IssuanceScriptCodes.Claim.BAD_ARGUMENT);
        assertThat(stock()).isEqualTo(TOTAL);
    }

    @ParameterizedTest(name = "[{index}] key={0} token={1}")
    @DisplayName("자기 파서가 파손이라 부를 값을 쓰기 전에 -10 으로 막는다")
    @MethodSource("badArguments")
    void rejectsArgumentsThatWouldWriteACorruptValue(String key, String token) {
        List<?> result = redis.execute(IssuanceScripts.CLAIM,
                List.of(STOCK, ISSUED, META, ISSUED_EVER),
                MEMBER, GRADE_BIT, key, token);

        assertThat(code(result)).isEqualTo(IssuanceScriptCodes.Claim.BAD_ARGUMENT);
        assertThat(stored()).isNull();
        assertThat(stock()).isEqualTo(TOTAL);
        assertThat(issuedEver()).isEqualTo(0);
    }

    static Stream<Arguments> badArguments() {
        return Stream.of(
                Arguments.of("", TOKEN_A),
                Arguments.of("key-1", ""),
                Arguments.of("key-1", "api|1-n1-t1-1")
        );
    }

    @Test
    @DisplayName("공백뿐인 토큰은 인자 이상이 아니다 — Lua 와 Java 가 함께 정상으로 읽는다")
    void whitespaceOnlyTokenIsAccepted() {
        assertThat(claim("key-1", "   ")).isEqualTo(IssuanceScriptCodes.Claim.OK);

        assertThatCode(() -> codec.decode(stored())).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "[{index}] token = \"{0}\"")
    @DisplayName("보상도 토큰 인자를 본다 — 빈 토큰을 '내 것이 아님(0)' 으로 삼키면 재고가 조용히 잠긴다")
    @ValueSource(strings = {"", "api|1-n1-t1-1"})
    void compensateRejectsUnusableToken(String token) {
        claim("key-1", TOKEN_A);
        String before = stored();

        long result = redis.execute(IssuanceScripts.COMPENSATE,
                List.of(STOCK, ISSUED, ISSUED_EVER), MEMBER, token);

        assertThat(result)
                .as("저장 토큰은 ([^|]+) 라 절대 이 값들과 같을 수 없다 — 항상 0 이 나오는 인자다")
                .isEqualTo(IssuanceScriptCodes.Compensate.BAD_ARGUMENT);
        assertThat(stored()).isEqualTo(before);
        assertThat(stock()).isEqualTo(TOTAL - 1);
    }

    @ParameterizedTest(name = "[{index}] token = \"{0}\"")
    @DisplayName("완료도 토큰 인자를 본다 — 남의 선점(-2) 과 인자 버그(-10) 는 다른 사건이다")
    @ValueSource(strings = {"", "api|1-n1-t1-1"})
    void completeRejectsUnusableToken(String token) {
        claim("key-1", TOKEN_A);
        String before = stored();

        long result = redis.execute(IssuanceScripts.COMPLETE, List.of(ISSUED), MEMBER, token);

        assertThat(result).isEqualTo(IssuanceScriptCodes.Complete.BAD_ARGUMENT);
        assertThat(stored()).isEqualTo(before);
    }

    @Test
    @DisplayName("빈 memberId 는 -10 이다 — 빈 값끼리 같은 field 를 공유한다")
    void rejectsEmptyMemberId() {
        List<?> result = redis.execute(IssuanceScripts.CLAIM,
                List.of(STOCK, ISSUED, META, ISSUED_EVER),
                "", GRADE_BIT, "key-1", TOKEN_A);

        assertThat(code(result)).isEqualTo(IssuanceScriptCodes.Claim.BAD_ARGUMENT);
        assertThat(stock()).isEqualTo(TOTAL);
    }

    @Test
    @DisplayName("완료·보상·회수도 빈 memberId 를 -10 으로 막는다 — 0(정상) 으로 삼키면 안 된다")
    void completeCompensateReclaimRejectEmptyMemberId() {
        claim("key-1", TOKEN_A);

        assertThat(redis.execute(IssuanceScripts.COMPLETE, List.of(ISSUED), "", TOKEN_A))
                .isEqualTo(IssuanceScriptCodes.Complete.BAD_ARGUMENT);
        assertThat(redis.execute(IssuanceScripts.COMPENSATE,
                List.of(STOCK, ISSUED, ISSUED_EVER), "", TOKEN_A))
                .as("HGET 이 false 라 '내 것이 아님(0)' 으로 나가면 재고가 조용히 잠긴다")
                .isEqualTo(IssuanceScriptCodes.Compensate.BAD_ARGUMENT);
        assertThat(redis.execute(IssuanceScripts.RECLAIM_CORRUPT,
                List.of(STOCK, ISSUED, ISSUED_EVER), "", "1", Integer.toString(TOTAL)))
                .isEqualTo(IssuanceScriptCodes.Reclaim.BAD_ARGUMENT);
        assertThat(stock()).isEqualTo(TOTAL - 1);
    }

    @Test
    @DisplayName("32비트를 넘는 등급 비트는 -10 이다 — bit.band 가 접어서 등급 게이트를 통과시킨다")
    void rejectsGradeBitBeyond32Bits() {
        List<?> result = redis.execute(IssuanceScripts.CLAIM,
                List.of(STOCK, ISSUED, META, ISSUED_EVER),
                MEMBER, Long.toString((1L << 32) + 1), "key-1", TOKEN_A);

        assertThat(code(result))
                .as("2^32+1 은 최하위 비트로 접혀 gradeMask 와 겹친다")
                .isEqualTo(IssuanceScriptCodes.Claim.BAD_ARGUMENT);
    }

    // ---------- 마감·미오픈보다 멱등이 먼저다 ----------

    @Test
    @DisplayName("마감 직후 같은 키 재시도는 -1 이 아니라 -6 이다 — DB 에 쿠폰이 있는데 실패 응답을 주면 안 된다")
    void replayOfDoneSurvivesTheGate() {
        claim("key-1", TOKEN_A);
        complete(TOKEN_A);
        givenGate("OPEN", testNow - HOUR, testNow - 1000, "7");

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.REPLAY_DONE);
    }

    @Test
    @DisplayName("마감 직후 같은 키의 처리 중 재시도는 -7 이다")
    void replayOfPendingSurvivesTheGate() {
        claim("key-1", TOKEN_A);
        givenGate("CLOSED", testNow - HOUR, testNow + HOUR, "7");

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.REPLAY_PENDING);
    }

    @Test
    @DisplayName("마감 후 다른 키로 오면 그대로 -1 이다 — 멱등이 게이트를 무력화하지 않는다")
    void freshKeyAfterCloseIsStillClosed() {
        claim("key-1", TOKEN_A);
        givenGate("CLOSED", testNow - HOUR, testNow + HOUR, "7");

        assertThat(claim("key-2", TOKEN_B)).isEqualTo(IssuanceScriptCodes.Claim.CLOSED);
        assertThat(stock()).isEqualTo(TOTAL - 1);
    }

    @Test
    @DisplayName("마감 회차에서도 파손은 -8 이다 — 경보가 게이트에 가려지면 안 된다")
    void corruptionIsReportedEvenAfterClose() {
        redis.opsForHash().put(ISSUED, MEMBER, "X|1|t|k");
        givenGate("CLOSED", testNow - HOUR, testNow + HOUR, "7");

        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.CORRUPT_VALUE);
    }

    @Test
    @DisplayName("미오픈·등급 미달도 같은 규칙이다")
    void replayBeatsNotOpenedAndGrade() {
        claim("key-1", TOKEN_A);
        complete(TOKEN_A);

        givenGate("OPEN", testNow + HOUR, testNow + 2 * HOUR, "7");
        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.REPLAY_DONE);

        givenGate("OPEN", testNow - HOUR, testNow + HOUR, "6");
        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.REPLAY_DONE);
    }

    // ---------- 경합 ----------

    @Test
    @DisplayName("재고 5장에 20명이 동시에 달려들면 정확히 5명만 성공한다")
    void concurrentClaimsNeverOverIssue() throws Exception {
        redis.opsForValue().set(STOCK, "5");

        List<Long> codes = inParallel(20, i -> {
            List<?> r = redis.execute(IssuanceScripts.CLAIM,
                    List.of(STOCK, ISSUED, META, ISSUED_EVER),
                    "m" + i, GRADE_BIT, "key-" + i, "tok-" + i);
            return code(r);
        });

        assertThat(codes.stream().filter(c -> c == IssuanceScriptCodes.Claim.OK).count()).isEqualTo(5);
        assertThat(codes.stream().filter(c -> c == IssuanceScriptCodes.Claim.SOLD_OUT).count()).isEqualTo(15);
        assertThat(stock()).isEqualTo(0);
        assertThat(issuedEver()).as("issued_ever 는 성공 수와 같아야 한다").isEqualTo(5);
        assertThat(redis.opsForHash().size(ISSUED))
                .as("매진 롤백이 자기 field 만 지웠는지 — 남았으면 남의 것을 지운 것이다")
                .isEqualTo(5);
    }

    @Test
    @DisplayName("같은 회원·같은 키로 동시에 들어오면 하나만 선점하고 나머지는 -7 이다")
    void concurrentReplaysClaimOnce() throws Exception {
        List<Long> codes = inParallel(12, i -> claim("key-1", "tok-" + i));

        assertThat(codes.stream().filter(c -> c == IssuanceScriptCodes.Claim.OK).count()).isEqualTo(1);
        assertThat(codes.stream().filter(c -> c == IssuanceScriptCodes.Claim.REPLAY_PENDING).count())
                .isEqualTo(11);
        assertThat(stock()).isEqualTo(TOTAL - 1);
        assertThat(issuedEver()).isEqualTo(1);
    }

    @Test
    @DisplayName("완료 CAS 가 동시에 여러 번 와도 승격은 한 번뿐이다")
    void concurrentCompletesPromoteOnce() throws Exception {
        claim("key-1", TOKEN_A);

        List<Long> codes = inParallel(12, i -> complete(TOKEN_A));

        assertThat(codes.stream().filter(c -> c == IssuanceScriptCodes.Complete.PROMOTED).count())
                .isEqualTo(1);
        assertThat(codes.stream().filter(c -> c == IssuanceScriptCodes.Complete.ALREADY_DONE).count())
                .isEqualTo(11);
    }

    @Test
    @DisplayName("복원 요청이 헤드룸보다 많으면 딱 헤드룸만큼만 성공한다")
    void restoreRaceRespectsCapUnderPressure() throws Exception {
        redis.execute(IssuanceScripts.CLAIM, List.of(STOCK, ISSUED, META, ISSUED_EVER),
                "m1", GRADE_BIT, "key-1", "tok-1");
        redis.execute(IssuanceScripts.CLAIM, List.of(STOCK, ISSUED, META, ISSUED_EVER),
                "m2", GRADE_BIT, "key-2", "tok-2");
        assertThat(stock()).isEqualTo(TOTAL - 2);

        List<Long> results = inParallel(6, i -> restore("1"));

        assertThat(results.stream().filter(r -> r == IssuanceScriptCodes.Restore.RESTORED).count())
                .as("헤드룸이 2 인데 6개가 동시에 왔다 — 상한 검사를 지우면 6개 다 성공한다")
                .isEqualTo(2);
        assertThat(results.stream().filter(r -> r == IssuanceScriptCodes.Restore.OVER_CAP).count())
                .isEqualTo(4);
        assertThat(stock()).isEqualTo(TOTAL);
    }

    @Test
    @DisplayName("완료와 보상이 동시에 오면 정확히 하나만 이기고 상태가 그 답과 일치한다")
    void completeAndCompensateNeverBothWin() throws Exception {
        claim("key-1", TOKEN_A);

        List<Long> results = inParallel(12, i -> i % 2 == 0
                ? complete(TOKEN_A)
                : redis.execute(IssuanceScripts.COMPENSATE,
                        List.of(STOCK, ISSUED, ISSUED_EVER), MEMBER, TOKEN_A));

        // 두 성공 코드가 똑같이 1 이라, 평평한 목록에서 값으로 세면 같은 원소를 두 번 센다.
        // inParallel 은 작업 순서대로 결과를 돌려주므로 짝/홀 인덱스로 가른다.
        long promoted = IntStream.range(0, results.size())
                .filter(i -> i % 2 == 0 && results.get(i) == IssuanceScriptCodes.Complete.PROMOTED)
                .count();
        long reverted = IntStream.range(0, results.size())
                .filter(i -> i % 2 == 1 && results.get(i) == IssuanceScriptCodes.Compensate.REVERTED)
                .count();
        assertThat(promoted + reverted)
                .as("둘 다 성공하면 지워진 자리가 되살아나고 재고가 한 장 늘어난다")
                .isEqualTo(1);

        if (promoted == 1) {
            assertThat(stored()).startsWith("D|");
            assertThat(stock()).isEqualTo(TOTAL - 1);
            assertThat(issuedEver()).isEqualTo(1);
        } else {
            assertThat(stored()).isNull();
            assertThat(stock()).isEqualTo(TOTAL);
            assertThat(issuedEver()).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("발급과 배치 복원이 동시에 돌아도 stock 이 총재고를 넘지 않는다")
    void claimAndRestoreNeverExceedTotal() throws Exception {
        for (int i = 0; i < 2; i++) {
            redis.execute(IssuanceScripts.CLAIM, List.of(STOCK, ISSUED, META, ISSUED_EVER),
                    "seed" + i, GRADE_BIT, "seed-key-" + i, "seed-tok-" + i);
        }
        assertThat(stock()).isEqualTo(TOTAL - 2);

        // 헤드룸은 2인데 복원 요청이 4건 × 3장이다. 상한 검사를 지우면 선점이 아무리 많아도
        // 8 - 4 + 12 = 16 이라 TOTAL 을 반드시 넘는다 — 그때 빨강이어야 한다.
        List<Long> results = inParallel(8, i -> i % 2 == 0
                ? code(redis.execute(IssuanceScripts.CLAIM,
                        List.of(STOCK, ISSUED, META, ISSUED_EVER),
                        "m" + i, GRADE_BIT, "key-" + i, "tok-" + i))
                : restore("3"));

        long claimed = IntStream.range(0, results.size())
                .filter(i -> i % 2 == 0 && results.get(i) == IssuanceScriptCodes.Claim.OK)
                .count();
        long restored = IntStream.range(0, results.size())
                .filter(i -> i % 2 == 1 && results.get(i) == IssuanceScriptCodes.Restore.RESTORED)
                .count();
        assertThat(stock()).as("총재고를 넘는 순간 초과 발급이 확정된다").isLessThanOrEqualTo(TOTAL);
        assertThat(stock())
                .as("재고는 선점만큼 줄고 복원만큼 는다 — 어느 쪽도 상대의 실행 중간을 보지 않는다")
                .isEqualTo(TOTAL - 2 - claimed + 3 * restored);
    }

    @Test
    @DisplayName("파손 회수와 같은 회원의 발급이 겹쳐도 재고가 늘지 않는다")
    void reclaimRacingWithClaimNeverInflatesStock() throws Exception {
        claim("key-1", TOKEN_A);
        redis.opsForHash().put(ISSUED, MEMBER, "X|1|t|k");
        long before = stock();

        inParallel(8, i -> i % 2 == 0
                ? reclaim(true)
                : claim("key-2", "tok-" + i));

        assertThat(stock())
                .as("회수가 되돌린 재고를 그 자리 선점이 다시 깎거나, 둘 다 실패하거나 둘 중 하나다")
                .isBetween(before, (long) TOTAL);
        if (stored() != null) {
            assertThatCode(() -> codec.decode(stored()))
                    .as("경합 뒤에도 파손 값이 새로 생기지 않는다")
                    .doesNotThrowAnyException();
        }
    }

    private List<Long> inParallel(int count, java.util.function.IntFunction<Long> task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(count);
        try {
            CyclicBarrier gate = new CyclicBarrier(count);
            List<Callable<Long>> jobs = IntStream.range(0, count)
                    .<Callable<Long>>mapToObj(i -> () -> {
                        gate.await();
                        return task.apply(i);
                    })
                    .toList();
            List<Long> out = new java.util.ArrayList<>();
            for (Future<Long> f : pool.invokeAll(jobs)) {
                out.add(f.get());
            }
            return out;
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("인자가 모자라면 Lua 런타임 에러가 아니라 -10 이다 — 장애로 오진되면 삼켜진다")
    void missingArgumentsAreRejectedNotThrown() {
        List<?> claimResult = redis.execute(IssuanceScripts.CLAIM,
                List.of(STOCK, ISSUED, META, ISSUED_EVER), MEMBER, GRADE_BIT, "key-1");
        assertThat(code(claimResult)).isEqualTo(IssuanceScriptCodes.Claim.BAD_ARGUMENT);

        assertThat(redis.execute(IssuanceScripts.COMPLETE, List.of(ISSUED), MEMBER))
                .isEqualTo(IssuanceScriptCodes.Complete.BAD_ARGUMENT);
        assertThat(redis.execute(IssuanceScripts.COMPENSATE,
                List.of(STOCK, ISSUED, ISSUED_EVER), MEMBER))
                .isEqualTo(IssuanceScriptCodes.Compensate.BAD_ARGUMENT);
        assertThat(redis.execute(IssuanceScripts.RECLAIM_CORRUPT,
                List.of(STOCK, ISSUED, ISSUED_EVER), MEMBER, "1"))
                .isEqualTo(IssuanceScriptCodes.Reclaim.BAD_ARGUMENT);
        assertThat(redis.execute(IssuanceScripts.RESTORE, List.of(STOCK, META)))
                .isEqualTo(IssuanceScriptCodes.Restore.BAD_ARGUMENT);
    }

    // ---------- 파손 판정: Java 와 Lua 가 같은 답 ----------

    @ParameterizedTest(name = "[{index}] 파손={1}")
    @DisplayName("Java codec 과 Lua 4종이 같은 입력에 같은 파손 판정을 낸다")
    @MethodSource("crossDecisionTable")
    void javaAndLuaAgreeOnCorruption(String value, boolean corrupt) {
        redis.opsForHash().put(ISSUED, MEMBER, value);
        long stockBefore = stock();
        long everBefore = issuedEver();

        if (corrupt) {
            assertThatThrownBy(() -> codec.decode(value))
                    .isInstanceOf(IssuedValueCorruptException.class);
        } else {
            assertThatCode(() -> codec.decode(value)).doesNotThrowAnyException();
        }

        assertThat(claim("other-key", TOKEN_A) == IssuanceScriptCodes.Claim.CORRUPT_VALUE)
                .as("선점 -8").isEqualTo(corrupt);
        assertThat(complete(TOKEN_A) == IssuanceScriptCodes.Complete.CORRUPT_VALUE)
                .as("완료 -3").isEqualTo(corrupt);
        assertThat(compensate(TOKEN_A) == IssuanceScriptCodes.Compensate.CORRUPT_VALUE)
                .as("보상 -3").isEqualTo(corrupt);
        // 파손 "판정" 만 본다. 복원까지 시키면 상한(-2)이 섞여 판정이 흐려진다 — 플래그 '0' 은
        // field 만 지우는 경로라 파손이면 2, 멀쩡하면 -1 로 갈린다.
        assertThat(reclaim(false) == IssuanceScriptCodes.Reclaim.RECLAIMED_ONLY)
                .as("회수는 파손일 때만 지운다").isEqualTo(corrupt);

        assertThat(stock())
                .as("파손 판정은 카운터를 건드리지 않는다 — 재동기화의 입력이 오염된다")
                .isEqualTo(stockBefore);
        assertThat(issuedEver()).isEqualTo(everBefore);
    }

    /** 근거는 {@code docs/14-v2-phase0/08-테스트.md} 의 교차 판정 테이블 8행. */
    static Stream<Arguments> crossDecisionTable() {
        return Stream.of(
                Arguments.of("X|1|t|k", true),
                Arguments.of("P|1|t|", true),
                Arguments.of("P|1|t|   ", false),
                Arguments.of("P|1|t|order-1\ntrace-2", false),
                Arguments.of("P|12345678901234|t|k", true),
                Arguments.of("P|9999999999999|t|k", false),
                Arguments.of("P|1|   |k", false),
                Arguments.of("P|1||k", true)
        );
    }

    @Test
    @DisplayName("개행이 든 멱등키는 정상 선점이고, 같은 키의 재시도로 다시 만난다")
    void newlineInsideIdempotencyKeyIsValid() {
        assertThat(claim("order-1\ntrace-2", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.OK);

        assertThat(claim("order-1\ntrace-2", TOKEN_A))
                .isEqualTo(IssuanceScriptCodes.Claim.REPLAY_PENDING);
    }

    // ---------- 파손 회수 ----------

    @Test
    @DisplayName("DB 에 발급이 없으면 회수가 보상과 같은 범위를 되돌린다")
    void reclaimRestoresStockWhenIssuanceIsAbsent() {
        claim("key-1", TOKEN_A);
        redis.opsForHash().put(ISSUED, MEMBER, "X|1|t|k");

        assertThat(reclaim(true)).isEqualTo(IssuanceScriptCodes.Reclaim.RECLAIMED_AND_RESTORED);
        assertThat(stored()).isNull();
        assertThat(stock()).isEqualTo(TOTAL);
        assertThat(issuedEver()).isEqualTo(0);

        assertThat(claim("key-1", TOKEN_B)).isEqualTo(IssuanceScriptCodes.Claim.OK);
    }

    @Test
    @DisplayName("DB 에 발급이 있으면 field 만 지운다 — 재고를 되살리면 초과 발급이다")
    void reclaimKeepsStockWhenIssuancePersisted() {
        claim("key-1", TOKEN_A);
        complete(TOKEN_A);
        redis.opsForHash().put(ISSUED, MEMBER, "D|1|tok|");

        assertThat(reclaim(false)).isEqualTo(IssuanceScriptCodes.Reclaim.RECLAIMED_ONLY);
        assertThat(stored()).isNull();
        assertThat(stock()).as("DB 에 있는 발급의 재고를 되살리면 그 자리를 남이 가져간다")
                .isEqualTo(TOTAL - 1);
        assertThat(issuedEver()).isEqualTo(1);
    }

    @ParameterizedTest(name = "[{index}] flag = {0}")
    @DisplayName("복원 여부 플래그가 0·1 이 아니면 -10 이고 아무것도 안 한다")
    @ValueSource(strings = {"", "2", "true", "yes"})
    void reclaimRejectsUnknownRestoreFlag(String flag) {
        claim("key-1", TOKEN_A);
        redis.opsForHash().put(ISSUED, MEMBER, "X|1|t|k");

        long result = redis.execute(IssuanceScripts.RECLAIM_CORRUPT,
                List.of(STOCK, ISSUED, ISSUED_EVER), MEMBER, flag, Integer.toString(TOTAL));

        assertThat(result).isEqualTo(IssuanceScriptCodes.Reclaim.BAD_ARGUMENT);
        assertThat(stored()).isEqualTo("X|1|t|k");
        assertThat(stock()).isEqualTo(TOTAL - 1);
    }

    @Test
    @DisplayName("회수도 상한을 넘으면 -2 다 — 파손 값에는 재고를 깎았다는 증거가 없다")
    void reclaimRefusesToExceedTotal() {
        redis.opsForHash().put(ISSUED, MEMBER, "X|1|t|k");

        assertThat(stock()).isEqualTo(TOTAL);
        assertThat(reclaim(true))
                .as("CLAIM 은 -10 가드 때문에 파손 값을 못 만든다 — 회수 대상은 짝이 증명되지 않는다")
                .isEqualTo(IssuanceScriptCodes.Reclaim.OVER_CAP);
        assertThat(stock()).isEqualTo(TOTAL);
        assertThat(stored()).as("상한에 걸리면 아무것도 하지 않는다").isEqualTo("X|1|t|k");
    }

    @Test
    @DisplayName("상한 안이면 회수가 그대로 되돌린다")
    void reclaimRestoresWhenWithinCap() {
        claim("key-1", TOKEN_A);
        redis.opsForHash().put(ISSUED, MEMBER, "X|1|t|k");

        assertThat(reclaim(true)).isEqualTo(IssuanceScriptCodes.Reclaim.RECLAIMED_AND_RESTORED);
        assertThat(stock()).isEqualTo(TOTAL);
    }

    @Test
    @DisplayName("회수는 멀쩡한 선점을 지우지 않는다 — 그건 초과 발급 방향이다")
    void reclaimRefusesLivingClaims() {
        claim("key-1", TOKEN_A);
        String before = stored();

        assertThat(reclaim(true)).isEqualTo(IssuanceScriptCodes.Reclaim.NOT_CORRUPT);
        assertThat(stored()).isEqualTo(before);
        assertThat(stock()).isEqualTo(TOTAL - 1);
    }

    @Test
    @DisplayName("회수의 총재고 인자가 숫자가 아니면 -10 이다")
    void reclaimRejectsUnusableTotal() {
        redis.opsForHash().put(ISSUED, MEMBER, "X|1|t|k");

        long result = redis.execute(IssuanceScripts.RECLAIM_CORRUPT,
                List.of(STOCK, ISSUED, ISSUED_EVER), MEMBER, "1", "nope");

        assertThat(result).isEqualTo(IssuanceScriptCodes.Reclaim.BAD_ARGUMENT);
        assertThat(stored()).isEqualTo("X|1|t|k");
    }

    @Test
    @DisplayName("회수할 field 가 없으면 0 이다")
    void reclaimOnMissingFieldIsNoop() {
        assertThat(reclaim(true)).isEqualTo(IssuanceScriptCodes.Reclaim.NOTHING);
        assertThat(stock()).isEqualTo(TOTAL);
    }

    // ---------- 배치 복원 ----------

    @Test
    @DisplayName("배치 복원이 stock + n > total 이면 -2 이고 stock 이 불변이다")
    void batchRestoreRejectsOverCap() {
        redis.opsForValue().set(STOCK, "8");

        assertThat(restore("3")).isEqualTo(IssuanceScriptCodes.Restore.OVER_CAP);
        assertThat(stock()).isEqualTo(8);

        assertThat(restore("2")).isEqualTo(IssuanceScriptCodes.Restore.RESTORED);
        assertThat(stock()).isEqualTo(TOTAL);
    }

    @ParameterizedTest(name = "[{index}] totalQuantity = {0}")
    @DisplayName("총재고가 canonical 정수가 아니면 게이트를 믿지 않는다 — 작성자 계약을 드러낸다")
    @ValueSource(strings = {"1e9", "10.0", " 10 ", "0x10", "007", "-0"})
    void nonCanonicalTotalIsNotReady(String total) {
        redis.opsForHash().put(META, "totalQuantity", total);
        redis.opsForValue().set(STOCK, "5");

        assertThat(restore("1")).isEqualTo(IssuanceScriptCodes.Restore.NOT_READY);
        assertThat(claim("key-1", TOKEN_A)).isEqualTo(IssuanceScriptCodes.Claim.NOT_READY);
        assertThat(redis.opsForValue().get(STOCK)).isEqualTo("5");
    }

    @ParameterizedTest(name = "[{index}] 빠진 필드 = {0}")
    @DisplayName("복원도 meta 다섯 필드를 요구한다 — 부분 상태는 재구성 창이라 건너뛴다")
    @ValueSource(strings = {"status", "openAt", "closeAt", "gradeMask"})
    void partialGateSkipsRestore(String missingField) {
        redis.opsForValue().set(STOCK, "5");
        redis.opsForHash().delete(META, missingField);

        assertThat(restore("1"))
                .as("복원분이 재구성의 stock 재계산에 덮여 유실되고 -1 카운터에도 안 잡힌다")
                .isEqualTo(IssuanceScriptCodes.Restore.NOT_READY);
        assertThat(redis.opsForValue().get(STOCK)).isEqualTo("5");
    }

    @Test
    @DisplayName("게이트가 없거나 totalQuantity 가 없으면 복원은 -1 이다")
    void batchRestoreWithoutGateIsNotReady() {
        redis.opsForValue().set(STOCK, "5");
        redis.delete(META);
        assertThat(restore("1")).isEqualTo(IssuanceScriptCodes.Restore.NOT_READY);

        redis.opsForHash().put(META, "status", "OPEN");
        assertThat(restore("1")).isEqualTo(IssuanceScriptCodes.Restore.NOT_READY);
        assertThat(stock()).isEqualTo(5);
    }

    @Test
    @DisplayName("복원도 stock 부재를 인자 이상이 아니라 -11 로 본다")
    void batchRestoreWithoutStockIsStockMissing() {
        redis.delete(STOCK);

        assertThat(restore("1")).isEqualTo(IssuanceScriptCodes.Restore.STOCK_MISSING);
    }

    @ParameterizedTest(name = "[{index}] n = {0}")
    @DisplayName("건수가 0·음수·비정수·숫자 아님이면 -3 이고 stock 이 불변이다")
    @ValueSource(strings = {"0", "-1", "2.5", "nope", "1e3", "0x10", " 3 ", "+3", "007"})
    void batchRestoreRejectsBadCounts(String count) {
        redis.opsForValue().set(STOCK, "5");

        assertThat(restore(count)).isEqualTo(IssuanceScriptCodes.Restore.BAD_ARGUMENT);
        assertThat(stock()).as("INCRBY 가 터지면 S6 이 Redis 장애로 오인해 삼킨다").isEqualTo(5);
    }

    // ---------- 헬퍼 ----------

    private void givenOpenGate() {
        givenGate("OPEN", testNow - HOUR, testNow + HOUR, "7");
    }

    private void givenGate(String status, long openAt, long closeAt, String gradeMask) {
        redis.delete(META);
        redis.opsForHash().putAll(META, Map.of(
                "status", status,
                "openAt", Long.toString(openAt),
                "closeAt", Long.toString(closeAt),
                "gradeMask", gradeMask,
                "totalQuantity", Integer.toString(TOTAL)));
    }

    private long claimedAt() {
        return codec.decode(stored()).claimedAtEpochMillis();
    }

    private List<?> claimResult(String idempotencyKey, String requestToken) {
        return redis.execute(IssuanceScripts.CLAIM,
                List.of(STOCK, ISSUED, META, ISSUED_EVER),
                MEMBER, GRADE_BIT, idempotencyKey, requestToken);
    }

    private long claim(String idempotencyKey, String requestToken) {
        return code(claimResult(idempotencyKey, requestToken));
    }

    private static long code(List<?> result) {
        return ((Number) result.get(0)).longValue();
    }

    private long complete(String requestToken) {
        return redis.execute(IssuanceScripts.COMPLETE, List.of(ISSUED), MEMBER, requestToken);
    }

    private long compensate(String requestToken) {
        return redis.execute(IssuanceScripts.COMPENSATE,
                List.of(STOCK, ISSUED, ISSUED_EVER), MEMBER, requestToken);
    }

    private long reclaim(boolean restoreStock) {
        return redis.execute(IssuanceScripts.RECLAIM_CORRUPT,
                List.of(STOCK, ISSUED, ISSUED_EVER),
                MEMBER, restoreStock ? "1" : "0", Integer.toString(TOTAL));
    }

    private long restore(String count) {
        return redis.execute(IssuanceScripts.RESTORE, List.of(STOCK, META), count);
    }

    private String stored() {
        return (String) redis.opsForHash().get(ISSUED, MEMBER);
    }

    private long stock() {
        return Long.parseLong(redis.opsForValue().get(STOCK));
    }

    private long issuedEver() {
        return Long.parseLong(redis.opsForValue().get(ISSUED_EVER));
    }
}
