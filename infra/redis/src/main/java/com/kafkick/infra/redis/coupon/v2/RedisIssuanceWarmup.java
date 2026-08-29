package com.kafkick.infra.redis.coupon.v2;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.coupon.v2.IssuedValue;
import com.kafkick.core.coupon.v2.IssuedValueCodec;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.coupon.v2.port.RestorationHaltStore;
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

    private static final Logger log = LoggerFactory.getLogger(RedisIssuanceWarmup.class);

    /**
     * 재구성으로 들어온 값의 요청토큰과 멱등키 자리. {@code '|'} 를 포함하지 않아 4필드 codec 을
     * 깨지 않는다.
     *
     * <p><b>두 자리의 안전성이 다르다.</b> 요청토큰은 {@code RequestTokenGenerator} 가 만들어
     * 클라이언트가 고를 수 없으므로 이 값과 겹치지 않는다 — 완료·보상 CAS 가 토큰을 비교하는데,
     * 겹칠 수 있었다면 남의 선점을 건드리는 토큰이 됐을 것이다.
     *
     * <p><b>멱등키는 다르다. 클라이언트가 이 값을 그대로 보낼 수 있고, 그것을 막는 코드가 없다.</b>
     * 보내면 선점 Lua 의 멱등키 비교가 통과해 {@code -6}(REPLAY_DONE)이 나가고,
     * {@code idempotency_records} 에 대응 행이 없어 500 이 된다. v1 발급은
     * {@code IdempotencyKeys.validate} 로 UUID v4 를 강제하는데 <b>v2 경로에는 그 호출이 없다</b>.
     * 재고나 1인 1매에는 영향이 없는 가용성 결함이고, 막는 자리가 이 모듈이 아니라 API 입력
     * 검증과 선점 Lua 의 인자 가드 둘이라 여기서 고치지 않았다 — {@code 10-작업분할.md} 의
     * "다음 단위로 넘긴 부채" 를 보라. <b>마커를 다른 문자열로 바꾸는 것은 해법이 아니다.</b>
     */
    static final String REBUILT_MARKER = "__rebuilt__";

    static final int HSET_BATCH_SIZE = 1_000;

    private final StringRedisTemplate redisTemplate;
    private final RestorationHaltStore haltStore;
    private final IssuedValueCodec codec = new IssuedValueCodec();

    /**
     * 복원 중단 표식은 <b>포트로</b> 지운다. 키를 여기서 직접 만들면 표식의 이름이 두 곳이
     * 되고, 한쪽만 바뀌었다는 사실은 "재구성했는데 만료가 안 돈다" 로만 드러난다.
     */
    public RedisIssuanceWarmup(StringRedisTemplate redisTemplate, RestorationHaltStore haltStore) {
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate");
        this.haltStore = Objects.requireNonNull(haltStore, "haltStore");
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
        // 재고를 다시 세운 것이 곧 "어긋남을 되돌렸다" 이므로 복원 중단 표식도 여기서 푼다.
        // 사람이 따로 눌러야 하는 해제 버튼을 두면 "고쳤는데 만료가 안 돈다" 가 된다.
        try {
            haltStore.clear(couponRoundId);
        } catch (RuntimeException failure) {
            // 여기까지 왔으면 재구성은 사실상 끝났다 — 카운터가 이미 다시 서 있다. 이 한
            // 줄 때문에 시딩 전체를 예외로 끝내면 뒤 단계(엔진 잠금·재고 갱신)가 안 돌아
            // "재고는 맞는데 게이트는 닫힌" 상태가 남는다. TTL 이 뒤를 받친다.
            log.error("재구성이 복원 중단 표식을 지우지 못했습니다. TTL 까지 그 회차 만료가 "
                    + "멈춰 있습니다. couponRoundId={}", couponRoundId, failure);
        }
    }

    @Override
    public void setRemainingStock(long couponRoundId, long remainingStock) {
        redisTemplate.opsForValue()
                .set(IssuanceKeys.of(couponRoundId).stock(), Long.toString(remainingStock));
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
