package com.kafkick.core.consistency;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsistencyRawValuesTest {

    @Test
    void rejectsNegativeRawValues() {
        assertThatThrownBy(() -> values(-1, 0, 0, 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> values(0, 0, -1, 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> values(0, 0, 0, -1, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> values(0, 0, 0, 0, -1, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> values(0, 0, 0, 0, 0, -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void allowsDriftValuesThatMustRemainObservable() {
        assertThatCode(() -> values(10, -1, 15, 9, 11, 8, -1))
                .doesNotThrowAnyException();
    }

    private static ConsistencyRawValues values(
            long totalQuantity,
            long redisRemaining,
            long redisIssuedEverCount,
            long redisMemberEverCount,
            long dbActiveCount,
            long dbIssuedEverCount,
            long storedActiveCount
    ) {
        return new ConsistencyRawValues(
                totalQuantity,
                redisRemaining,
                redisIssuedEverCount,
                redisMemberEverCount,
                dbActiveCount,
                dbIssuedEverCount,
                storedActiveCount
        );
    }
}
