package com.kafkick.infra.redis.coupon.v2;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Collection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 게이트를 닫는 명령이 <b>{@code UNLINK} 인지</b>만 본다.
 *
 * <p>결과로는 갈리지 않는다 — {@code DEL} 도 키를 지운다. 갈리는 것은 <b>지우는 동안 Redis
 * 단일 스레드가 서느냐</b>이고(§3.3: 175k field 기준 DEL 11.6ms · UNLINK 1ms 미만), 그 차이는
 * 값이 아니라 호출한 명령에만 남는다. 그래서 여기만 대역을 쓴다.
 */
class RedisIssuanceGateCloseGateTest {

    private static final long ROUND_ID = 42;

    @Test
    @DisplayName("meta 를 UNLINK 로 지운다 — DEL 이면 그 시간만큼 발급 전체가 선다")
    void unlinksMetaInsteadOfDeleting() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        RedisIssuanceGate gate = new RedisIssuanceGate(
                mock(IssuanceScriptRunner.class), redisTemplate);

        gate.closeGate(ROUND_ID);

        verify(redisTemplate).unlink(eq(IssuanceKeys.of(ROUND_ID).meta()));
        verify(redisTemplate, never()).delete(anyString());
        // 카운터 세 키는 건드리지 않는다 — 지우는 것은 시딩의 몫이다.
        verify(redisTemplate, never()).unlink(org.mockito.ArgumentMatchers.<Collection<String>>any());
    }
}
