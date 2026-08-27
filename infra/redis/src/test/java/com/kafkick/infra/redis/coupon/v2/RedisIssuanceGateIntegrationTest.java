package com.kafkick.infra.redis.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.coupon.v2.IssuedValue;
import com.kafkick.core.coupon.v2.IssuedValueCodec;
import com.kafkick.core.coupon.v2.port.ClaimCommand;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.coupon.v2.port.CompleteOutcome;
import com.kafkick.core.coupon.v2.port.GateMeta;
import com.kafkick.core.coupon.v2.port.GateStatus;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.ReclaimOutcome;
import com.kafkick.core.coupon.v2.port.RestoreOutcome;

/**
 * 어댑터 통합 테스트. <b>키 리터럴을 베끼지 않는다</b> — 키의 출처는 {@link IssuanceKeys} 한 곳이고
 * 테스트도 그쪽을 쓴다. 리터럴을 옮겨 적으면 어댑터가 키를 바꿔도 테스트는 계속 초록이고,
 * 그 사실은 정합성 리더가 아무것도 못 읽을 때에야 드러난다.
 *
 * <p>스크립트가 무엇을 돌려주는지는 계약 테스트({@code IssuanceScriptContractTest})가 본다.
 * 여기서 보는 것은 <b>어댑터가 그 코드를 어떤 결과 타입으로 옮기는가</b>와
 * <b>키 리스트를 스크립트마다 맞게 넘기는가</b> 둘이다.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisIssuanceGateIntegrationTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static final long ROUND_ID = 7;
    private static final long MEMBER_ID = 42;
    private static final int GRADE_BIT = 1;
    private static final int GRADE_MASK = 0b111;
    private static final String TOKEN_A = "api-1-n1-t1-1";
    private static final String TOKEN_B = "api-2-n7-t3-9";
    private static final String KEY_A = "idem-a";
    private static final String KEY_B = "idem-b";
    private static final long TOTAL = 10;
    private static final long HOUR = 3_600_000L;

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;

    private final IssuanceKeys keys = IssuanceKeys.of(ROUND_ID);
    private final IssuedValueCodec codec = new IssuedValueCodec();
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
        redis.delete(List.of(keys.stock(), keys.issued(), keys.meta(), keys.issuedEver()));
        redis.opsForValue().set(keys.stock(), Long.toString(TOTAL));
        redis.opsForValue().set(keys.issuedEver(), "0");
        gate.writeMeta(ROUND_ID, openGate());
    }

    private static GateMeta openGate() {
        long now = System.currentTimeMillis();
        return new GateMeta(GateStatus.OPEN, now - HOUR, now + HOUR, GRADE_MASK, TOTAL);
    }

    private ClaimResult claim(long memberId, String idempotencyKey, String token) {
        return gate.claim(new ClaimCommand(ROUND_ID, memberId, GRADE_BIT, idempotencyKey, token));
    }

    @Test
    @DisplayName("키 이름과 해시태그의 출처는 어댑터 한 곳이다")
    void keysCarryHashTag() {
        assertThat(keys.stock()).isEqualTo("cy:v2:stock:{7}");
        assertThat(keys.issued()).isEqualTo("cy:v2:issued:{7}");
        assertThat(keys.meta()).isEqualTo("cy:v2:meta:{7}");
        assertThat(keys.issuedEver()).isEqualTo("cy:v2:issued_ever:{7}");
    }

    @Test
    @DisplayName("meta 를 다섯 필드로 쓰고 그대로 읽는다")
    void writesFiveMetaFields() {
        GateMeta written = openGate();
        gate.writeMeta(ROUND_ID, written);

        Map<Object, Object> stored = redis.opsForHash().entries(keys.meta());
        assertThat(stored).containsOnlyKeys(
                "status", "openAt", "closeAt", "gradeMask", "totalQuantity");
        assertThat(gate.readMeta(ROUND_ID)).contains(written);
    }

    @Test
    @DisplayName("meta 가 부분 상태면 읽기는 비어 있고 선점은 미준비다")
    void partialMetaIsNotReady() {
        redis.opsForHash().delete(keys.meta(), "totalQuantity");

        assertThat(gate.readMeta(ROUND_ID)).isEqualTo(Optional.empty());
        assertThat(claim(MEMBER_ID, KEY_A, TOKEN_A).outcome()).isEqualTo(ClaimOutcome.GATE_NOT_READY);
    }

    @Test
    @DisplayName("선점 성공은 잔여 재고를 값 객체로 돌려준다")
    void claimReturnsRemainingStock() {
        ClaimResult result = claim(MEMBER_ID, KEY_A, TOKEN_A);

        assertThat(result.outcome()).isEqualTo(ClaimOutcome.CLAIMED);
        assertThat(result.remainingStock()).isEqualTo(TOTAL - 1);
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo(Long.toString(TOTAL - 1));
        assertThat(redis.opsForValue().get(keys.issuedEver())).isEqualTo("1");

        IssuedValue stored = codec.decode(
                (String) redis.opsForHash().get(keys.issued(), Long.toString(MEMBER_ID)));
        assertThat(stored.status()).isEqualTo(IssuedValue.Status.PENDING);
        assertThat(stored.requestToken()).isEqualTo(TOKEN_A);
        assertThat(stored.idempotencyKey()).isEqualTo(KEY_A);
    }

    @Test
    @DisplayName("거절 결과에서 잔여 재고를 꺼내면 실패한다 — 선점 성공에만 있는 값이다")
    void rejectedResultHasNoStock() {
        redis.opsForValue().set(keys.stock(), "0");

        ClaimResult result = claim(MEMBER_ID, KEY_A, TOKEN_A);

        assertThat(result.outcome()).isEqualTo(ClaimOutcome.SOLD_OUT);
        assertThatThrownBy(result::remainingStock).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("같은 회원의 다른 멱등키는 중복, 같은 키는 재시도다")
    void separatesDuplicateFromReplay() {
        claim(MEMBER_ID, KEY_A, TOKEN_A);

        assertThat(claim(MEMBER_ID, KEY_B, TOKEN_B).outcome()).isEqualTo(ClaimOutcome.DUP_PER_MEMBER);
        assertThat(claim(MEMBER_ID, KEY_A, TOKEN_B).outcome()).isEqualTo(ClaimOutcome.REPLAY_PENDING);
    }

    @Test
    @DisplayName("stock 이 자료형부터 틀리면 매진이 아니라 카운터 불가로 나가고 선점은 되돌아간다")
    void wrongTypeStockIsCounterUnreadable() {
        redis.delete(keys.stock());
        redis.opsForHash().put(keys.stock(), "f", "1");

        ClaimResult result = claim(MEMBER_ID, KEY_A, TOKEN_A);

        assertThat(result.outcome()).isEqualTo(ClaimOutcome.COUNTER_UNREADABLE);
        assertThat(redis.opsForHash().hasKey(keys.issued(), Long.toString(MEMBER_ID))).isFalse();
    }

    @Test
    @DisplayName("완료는 자기 선점일 때만 올라간다")
    void completePromotesOwnClaimOnly() {
        claim(MEMBER_ID, KEY_A, TOKEN_A);

        assertThat(gate.complete(ROUND_ID, MEMBER_ID, TOKEN_B)).isEqualTo(CompleteOutcome.FOREIGN_CLAIM);
        assertThat(gate.complete(ROUND_ID, MEMBER_ID, TOKEN_A)).isEqualTo(CompleteOutcome.PROMOTED);
        assertThat(gate.complete(ROUND_ID, MEMBER_ID, TOKEN_A)).isEqualTo(CompleteOutcome.ALREADY_DONE);
    }

    @Test
    @DisplayName("보상은 세 키를 되돌린다 — issued_ever 가 KEYS[3] 인 스크립트다")
    void compensateRevertsAllThree() {
        claim(MEMBER_ID, KEY_A, TOKEN_A);

        assertThat(gate.compensate(ROUND_ID, MEMBER_ID, TOKEN_A)).isEqualTo(CompensateOutcome.REVERTED);
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo(Long.toString(TOTAL));
        assertThat(redis.opsForValue().get(keys.issuedEver())).isEqualTo("0");
        assertThat(redis.opsForHash().hasKey(keys.issued(), Long.toString(MEMBER_ID))).isFalse();
    }

    @Test
    @DisplayName("완료된 선점을 보상하면 거부한다")
    void compensateOnDoneIsRejected() {
        claim(MEMBER_ID, KEY_A, TOKEN_A);
        gate.complete(ROUND_ID, MEMBER_ID, TOKEN_A);

        assertThat(gate.compensate(ROUND_ID, MEMBER_ID, TOKEN_A))
                .isEqualTo(CompensateOutcome.ALREADY_DONE);
    }

    @Test
    @DisplayName("복원은 상한을 같은 원자 실행에서 본다")
    void restoreChecksCap() {
        claim(MEMBER_ID, KEY_A, TOKEN_A);

        assertThat(gate.restore(ROUND_ID, 1)).isEqualTo(RestoreOutcome.RESTORED);
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo(Long.toString(TOTAL));
        assertThat(gate.restore(ROUND_ID, 1)).isEqualTo(RestoreOutcome.OVER_CAP);
    }

    @Test
    @DisplayName("파손 회수는 멀쩡한 값을 건드리지 않고, 파손 값만 되돌린다")
    void reclaimTouchesCorruptOnly() {
        claim(MEMBER_ID, KEY_A, TOKEN_A);

        assertThat(gate.reclaimCorrupt(ROUND_ID, MEMBER_ID, true, TOTAL))
                .isEqualTo(ReclaimOutcome.NOT_CORRUPT);

        redis.opsForHash().put(keys.issued(), Long.toString(MEMBER_ID), "X|1|t|k");
        assertThat(gate.reclaimCorrupt(ROUND_ID, MEMBER_ID, true, TOTAL))
                .isEqualTo(ReclaimOutcome.RECLAIMED_AND_RESTORED);
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo(Long.toString(TOTAL));
        assertThat(redis.opsForValue().get(keys.issuedEver())).isEqualTo("0");
    }

    @Test
    @DisplayName("DB 에 발급이 있으면 field 만 지운다")
    void reclaimKeepsStockWhenIssued() {
        claim(MEMBER_ID, KEY_A, TOKEN_A);
        redis.opsForHash().put(keys.issued(), Long.toString(MEMBER_ID), "X|1|t|k");

        assertThat(gate.reclaimCorrupt(ROUND_ID, MEMBER_ID, false, TOTAL))
                .isEqualTo(ReclaimOutcome.RECLAIMED_ONLY);
        assertThat(redis.opsForValue().get(keys.stock())).isEqualTo(Long.toString(TOTAL - 1));
    }
}
