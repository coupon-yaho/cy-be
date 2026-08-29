package com.kafkick.infra.redis.coupon.v2;

import java.time.Duration;
import java.util.Objects;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.coupon.v2.port.RestorationHaltStore;

/**
 * 복원 중단 표식의 Redis 어댑터. 키 이름의 출처는 {@link IssuanceKeys} 한 곳이다.
 *
 * <p><b>TTL 을 건다.</b> 표식을 지우는 경로는 재시딩({@code seedCounters}) 하나뿐인데, 재시딩은
 * 이미 열린 회차를 거절한다({@code GATE_ALREADY_OPEN}·{@code ROUND_ALREADY_OPENED}) — 그런데
 * {@code -2} 는 <b>열린 회차에서만</b> 난다. TTL 이 없으면 그 표식은 아무도 못 푸는
 * <b>단방향 래치</b>가 되고, 그 회차의 만료가 영구 정지해 {@code active_count} 가 영영 안 준다.
 * <b>지금 코드에서 TTL 은 유일한 해제 수단이다</b> — {@link #clear} 를 부르는 곳은 재시딩
 * 하나뿐이라 열린 회차에는 닿지 않는다. 살아있는 회차의 재구성 경로(S8b)나 운영 해제
 * 경로가 생기면 그쪽이 {@link #clear} 를 부르고, TTL 은 그때 최후의 안전망으로 내려간다.
 * 그 전까지는 아무도 대응하지 않으면 <b>TTL 주기마다 한 묶음씩 어긋남이 누적된다</b>.
 *
 * <p><b>다시 멈춰도 만료 시각은 안 밀린다</b>({@code SET NX}). 취소 경로도 같은 회차에서
 * 계속 {@code -2} 를 받으므로, 갱신하면 그 트래픽이 재판정을 무한정 미뤄 래치가 되살아난다.
 *
 * <p>만료가 <b>어긋남을 낫게 하지는 않는다.</b> {@code -2} 는 이벤트가 아니라 상태라, TTL 이
 * 지나면 배치가 한 청크를 다시 태워 재판정하고 어긋남이 그대로면 곧바로 다시 멈춘다. 대가는
 * TTL 주기마다 {@code transaction-size} 한 묶음이고, 그것도 과소 방향이다.
 */
public class RedisRestorationHaltStore implements RestorationHaltStore {

    private static final String HALTED = "1";

    /**
     * 표식의 수명. 경보를 받은 사람이 움직일 시간보다 넉넉하고, 그 사이 재판정 비용은
     * 한 시간에 {@code transaction-size} 한 묶음뿐이다.
     */
    public static final Duration TTL = Duration.ofHours(1);

    private final StringRedisTemplate redisTemplate;

    public RedisRestorationHaltStore(StringRedisTemplate redisTemplate) {
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate");
    }

    @Override
    public void halt(long couponRoundId) {
        // 이미 서 있으면 TTL 을 밀지 않는다. 표식을 세우는 경로는 만료 배치만이 아니라
        // 취소·사용취소도 있고(06 이 취소는 안 멈추기로 했다) 그쪽은 표식이 서 있어도 계속
        // -2 를 받는다 — 갱신하면 시간당 취소 한 건만으로 만료가 영원히 밀려, TTL 로 끊으려던
        // 단방향 래치가 그대로 성립한다. 만료 시각은 최초 중단 시점 기준이다.
        redisTemplate.opsForValue()
                .setIfAbsent(IssuanceKeys.of(couponRoundId).restorationHalt(), HALTED, TTL);
    }

    @Override
    public boolean isHalted(long couponRoundId) {
        return Boolean.TRUE.equals(
                redisTemplate.hasKey(IssuanceKeys.of(couponRoundId).restorationHalt()));
    }

    @Override
    public void clear(long couponRoundId) {
        redisTemplate.unlink(IssuanceKeys.of(couponRoundId).restorationHalt());
    }
}
