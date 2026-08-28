package com.kafkick.infra.redis.coupon.v2;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.coupon.v2.IssuedValue;
import com.kafkick.core.coupon.v2.IssuedValueCodec;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.coupon.v2.port.RebuiltIssued;

/**
 * 워밍업 시딩의 Redis 어댑터. <b>명령 규칙(설계 §3.3)이 여기서 지켜진다</b> — 회차 하나를
 * 올리는 이 연산이 발급과 같은 단일 스레드를 쓰기 때문이다.
 *
 * <p>{@code HSET} 을 {@value #HSET_BATCH_SIZE} 건으로 쪼갠다. 1만 건을 한 번에 밀면 그 시간
 * 전체가 뒤의 모든 명령을 세운다. 삭제는 {@code UNLINK} 다 — 같은 이유이고, {@code DEL} 은
 * 175k field 기준 11.6ms 를 한 스레드에서 태운다.
 */
public class RedisIssuanceWarmup implements IssuanceWarmupPort {

    /**
     * 재구성으로 들어온 값의 요청토큰과 멱등키 자리. <b>실제 요청의 것과 겹치면 안 된다.</b>
     * 완료·보상 CAS 가 토큰을 비교하므로, 겹치는 순간 남의 선점을 건드릴 수 있는 토큰이 된다.
     * {@code '|'} 를 포함하지 않아 4필드 codec 을 깨지 않는다.
     */
    static final String REBUILT_MARKER = "__rebuilt__";

    static final int HSET_BATCH_SIZE = 1_000;

    private final StringRedisTemplate redisTemplate;
    private final IssuedValueCodec codec = new IssuedValueCodec();

    public RedisIssuanceWarmup(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void seedCounters(
            long couponRoundId, List<RebuiltIssued> everMembers, long remainingStock) {
        IssuanceKeys keys = IssuanceKeys.of(couponRoundId);

        // 쓰기 전에 전부 인코딩한다. 값 하나가 형식을 깨는 것은 첫 배치를 밀기 전에 드러나야
        // 한다 — 중간에 터지면 issued 만 반쯤 찬 상태가 남고, 그 회차는 issued_ever 도 stock 도
        // 없이 게이트만 닫혀 있어 무엇이 잘못됐는지 아무 값도 말해 주지 않는다.
        Map<String, String> encoded = new LinkedHashMap<>(everMembers.size());
        for (RebuiltIssued member : everMembers) {
            String field = Long.toString(member.memberId());
            // Hash 는 같은 field 를 조용히 덮는다. 그대로 두면 HLEN < issued_ever 이고
            // 그 차가 곧 LUA_GAP 이라, 워밍업이 그 자체로 정합성 사고가 된다.
            if (encoded.putIfAbsent(field, encode(member)) != null) {
                throw new IllegalArgumentException(
                        "누적 회원 목록에 같은 회원이 두 번 들어 있습니다: " + member.memberId());
            }
        }

        // issued 와 issued_ever 를 **함께** 지운다. issued 만 지우면 중간에 죽었을 때
        // issued_ever 에 직전 회차 값이 남아 HLEN 과 갈라지고, 그 차가 곧 LUA_GAP 이다 —
        // 게이트는 닫혀 있어 발급 사고는 아니지만 크기와 무관하게 CRITICAL 이고, 원인이
        // "워밍업이 끊겼다" 라는 사실은 어디에도 안 남는다. 둘 다 없으면 리더가 예열로 읽는다.
        redisTemplate.unlink(List.of(keys.issued(), keys.issuedEver()));
        Map<String, String> pending = new LinkedHashMap<>(HSET_BATCH_SIZE);
        for (Map.Entry<String, String> entry : encoded.entrySet()) {
            pending.put(entry.getKey(), entry.getValue());
            if (pending.size() == HSET_BATCH_SIZE) {
                redisTemplate.opsForHash().putAll(keys.issued(), pending);
                pending.clear();
            }
        }
        if (!pending.isEmpty()) {
            redisTemplate.opsForHash().putAll(keys.issued(), pending);
        }
        // issued_ever 는 목록 크기다. 인자로 받지 않는 이유가 이 한 줄에 있다 — 두 값이
        // 갈라질 수 있으면 언젠가 갈라진다.
        redisTemplate.opsForValue().set(keys.issuedEver(), Integer.toString(encoded.size()));
        redisTemplate.opsForValue().set(keys.stock(), Long.toString(remainingStock));
    }

    private String encode(RebuiltIssued member) {
        return codec.encode(new IssuedValue(
                // DB 에 행이 있다는 것이 곧 영속 완료다. P 로 되살리면 PENDING 계측(05)이
                // 없는 미영속 건을 회차 내내 보고한다.
                IssuedValue.Status.DONE,
                member.claimedAtEpochMillis(),
                REBUILT_MARKER,
                REBUILT_MARKER));
    }

}
