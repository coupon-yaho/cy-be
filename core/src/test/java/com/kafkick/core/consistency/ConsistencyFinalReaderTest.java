package com.kafkick.core.consistency;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.SourceStatus;

class ConsistencyFinalReaderTest {

    @Test
    void singleReadDelegatesToTheBulkContract() {
        ConsistencyFinalObservation expected = new ConsistencyFinalObservation(SourceStatus.PENDING, null);
        List<Long> recordedIds = new ArrayList<>();
        ConsistencyFinalReader reader = couponIds -> {
            recordedIds.addAll(couponIds);
            Map<Long, ConsistencyFinalObservation> result = new LinkedHashMap<>();
            result.put(11L, expected);
            return result;
        };

        ConsistencyFinalObservation result = reader.findLatestByCouponId(11L);

        assertThat(result).isSameAs(expected);
        assertThat(recordedIds).containsExactly(11L);
    }
}
