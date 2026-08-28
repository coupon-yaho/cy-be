package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.kafkick.infra.redis.coupon.v2.IssuanceKeys;

class PendingIssuedGaugePropertiesTest {

    @Test
    void defaultIssuedKeyIsTheAdapterOwnedV2Key() {
        assertThat(properties(null).issuedKey(37L)).isEqualTo(IssuanceKeys.of(37L).issued());
    }

    @Test
    void onlyThePlaceholderIsSubstitutedSoAPrefixCarryingTheSameWordSurvives() {
        assertThat(properties("cy:obs:couponRoundId:{couponRoundId}").issuedKey(7L))
            .isEqualTo("cy:obs:couponRoundId:{7}");
    }

    @Test
    void substitutionKeepsTheHashTagBracesTheClusterSlotDependsOn() {
        assertThat(properties("cy:v2:issued:{couponRoundId}").issuedKey(7L))
            .isEqualTo(IssuanceKeys.of(7L).issued())
            .contains("{7}");
    }

    @Test
    void keyWithoutThePlaceholderIsRejectedAtBinding() {
        assertThatThrownBy(() -> properties("cy:v2:issued:couponRoundId"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    private static PendingIssuedGaugeProperties properties(String issuedKey) {
        return new PendingIssuedGaugeProperties(
            true, Duration.ofSeconds(30), Duration.ofMinutes(5), 200, issuedKey);
    }
}
