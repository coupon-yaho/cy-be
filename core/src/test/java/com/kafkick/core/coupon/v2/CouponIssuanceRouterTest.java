package com.kafkick.core.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.observation.EngineVersion;

class CouponIssuanceRouterTest {

    @Test
    void cachesOneImmutableDefinitionAndRoutesTheRoundToOnlyOneEngine() {
        CouponRoundIssuanceDefinitionRepository repository =
                mock(CouponRoundIssuanceDefinitionRepository.class);
        when(repository.lockAndFindById(147L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(147L, 7, EngineVersion.V2)));
        CouponRoundIssuanceDefinitionCache cache =
                new CouponRoundIssuanceDefinitionCache(repository);
        CouponIssuanceRouter router = new CouponIssuanceRouter(cache);
        AtomicInteger v1Calls = new AtomicInteger();
        AtomicInteger v2Calls = new AtomicInteger();

        String first = router.route(147L,
                definition -> { v1Calls.incrementAndGet(); return "v1"; },
                definition -> { v2Calls.incrementAndGet(); return "v2"; });
        String second = router.route(147L,
                definition -> { v1Calls.incrementAndGet(); return "v1"; },
                definition -> { v2Calls.incrementAndGet(); return "v2"; });

        assertThat(first).isEqualTo("v2");
        assertThat(second).isEqualTo("v2");
        assertThat(v1Calls).hasValue(0);
        assertThat(v2Calls).hasValue(2);
        verify(repository).lockAndFindById(147L);
    }

    @Test
    void nullEngineFromLegacyRoundRoutesToV1() {
        CouponRoundIssuanceDefinition definition =
                new CouponRoundIssuanceDefinition(1L, 3, null);

        assertThat(definition.engineVersion()).isEqualTo(EngineVersion.V1);
    }
}
