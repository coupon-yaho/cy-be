package com.kafkick.infra.redis.coupon.v2;

import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 스크립트별 KEYS 를 <b>스크립트마다 하나씩인 진입점</b> 안에 가둔다.
 *
 * <p>5종의 KEYS 순서는 제각각이다 — {@code issued_ever} 가 선점에서는 {@code KEYS[4]} 인데
 * 보상·회수에서는 {@code KEYS[3]} 이다. 키 리스트를 호출부가 만들면 그 순서를 사람이 기억해야
 * 하고, 한 번 틀리면 보상의 {@code readableCounters} 가 {@code issued_ever} 대신 {@code meta}
 * 를 읽어 <b>모든 보상이 영구히 {@code -11}</b> 이 된다 — 재고가 조용히 잠기는 방향이다.
 *
 * <p><b>순서를 통일하지 않고 타입으로 가둔 이유</b> — 통일은 Lua 5종 본문과 {@code docs/14} 의
 * 인자 가드 표, 그리고 S2 계약 테스트를 함께 흔든다. 설계 §0.1 이 구조 변경을 반려한 상태라
 * 그 판단은 이 단위의 몫이 아니다. 여기서는 <b>키 리스트를 만들 수 있는 길을 없애</b> 같은
 * 결함을 호출 단계에서 막는다: 호출부는 회차와 ARGV 만 넘긴다.
 *
 * <p>ARGV 를 {@code Object...} 로 받는 이유는 인자 가드가 <b>모자란 인자와 빈 문자열까지</b>
 * 계약이기 때문이다. 타입으로 조여 두면 그 분기를 테스트가 실행할 수 없다.
 */
public class IssuanceScriptRunner {

    private final StringRedisTemplate redisTemplate;

    public IssuanceScriptRunner(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 선점. 반환은 {@code {코드}} 이고 성공일 때만 {@code {0, 잔여재고}} 다.
     *
     * <p><b>{@code List<Long>} 으로 좁혀서 돌려주지 않는다.</b> 스크립트의 결과 타입은
     * {@code List.class} 라 원소 타입이 제네릭에 없고, 여기서 캐스팅해 봐야 검사되지 않는다 —
     * 원소가 {@code Long} 이 아닌 날 예외는 이 메서드가 아니라 <b>훨씬 뒤에서</b> 터진다.
     * 원소를 숫자로 읽는 책임은 그것을 해석하는 어댑터에 둔다.
     */
    public List<?> claim(long couponRoundId, Object... argv) {
        IssuanceKeys keys = IssuanceKeys.of(couponRoundId);
        return redisTemplate.execute(IssuanceScripts.CLAIM,
                List.of(keys.stock(), keys.issued(), keys.meta(), keys.issuedEver()), argv);
    }

    public long complete(long couponRoundId, Object... argv) {
        IssuanceKeys keys = IssuanceKeys.of(couponRoundId);
        return redisTemplate.execute(IssuanceScripts.COMPLETE, List.of(keys.issued()), argv);
    }

    public long compensate(long couponRoundId, Object... argv) {
        IssuanceKeys keys = IssuanceKeys.of(couponRoundId);
        return redisTemplate.execute(IssuanceScripts.COMPENSATE,
                List.of(keys.stock(), keys.issued(), keys.issuedEver()), argv);
    }

    public long restore(long couponRoundId, Object... argv) {
        IssuanceKeys keys = IssuanceKeys.of(couponRoundId);
        return redisTemplate.execute(IssuanceScripts.RESTORE,
                List.of(keys.stock(), keys.meta()), argv);
    }

    public long reclaimCorrupt(long couponRoundId, Object... argv) {
        IssuanceKeys keys = IssuanceKeys.of(couponRoundId);
        return redisTemplate.execute(IssuanceScripts.RECLAIM_CORRUPT,
                List.of(keys.stock(), keys.issued(), keys.issuedEver()), argv);
    }
}
