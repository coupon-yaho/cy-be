package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PartitionKeysTest {

    @Test
    @DisplayName("persist 는 issuanceCode · attempt 와 notify 는 memberId 를 키로 쓴다")
    void keysMatchTheDeclaredContract() {
        assertThat(PartitionKeys.forPersist("ISSUANCE0000001")).isEqualTo("ISSUANCE0000001");
        assertThat(PartitionKeys.forAttempt(77L)).isEqualTo("77");
        assertThat(PartitionKeys.forNotify(77L)).isEqualTo("77");
    }

    /**
     * 단일 캠페인 부하에서 회원이 다르면 키도 달라야 한다. 같은 값이 나오면 파티션이 몇 개든
     * 한 곳으로 몰린다 — {@code couponId} 를 키로 쓴 것과 같은 결과다.
     */
    @Test
    @DisplayName("같은 캠페인의 서로 다른 회원은 서로 다른 키를 받는다")
    void distinctMembersOfOneCampaignGetDistinctKeys() {
        assertThat(PartitionKeys.forAttempt(1L)).isNotEqualTo(PartitionKeys.forAttempt(2L));
    }

    @Test
    @DisplayName("키로 쓸 수 없는 값은 그 자리에서 거부한다")
    void rejectsValuesThatWouldSilentlyCollapseThePartitioning() {
        assertThatThrownBy(() -> PartitionKeys.forPersist(" "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PartitionKeys.forAttempt(0L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
