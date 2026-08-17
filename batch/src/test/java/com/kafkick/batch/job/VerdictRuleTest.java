// 판정 규칙을 값 단위로 고정합니다. 잡을 안 띄우고 분기만 봅니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.VerdictType;

/**
 * <b>CORRUPT 분기가 잡 테스트에서 통째로 빠져 있었다.</b> 잡 레벨 테스트는 전부 CLEAN 이고
 * CORRUPT 는 시작 가드에 막혀 판정까지 못 간다 — 조건문을 지우거나 뒤집어도 전부 초록이었다.
 *
 * <p>이 판정은 다음 티켓(정답 집합 대조)에서 <b>뜻이 바뀔 값</b>이라 더 그렇다.
 * 지금 무엇을 쓰기로 했는지가 코드로 남아 있어야 그때 무엇이 바뀌는지 보인다.
 */
class VerdictRuleTest {

    @Test
    @DisplayName("CLEAN 은 검출 0건이 합격이다")
    void passCleanRunWithoutFindings() {
        assertThat(VerifyJobConfig.verdictOf(DatasetType.CLEAN, 0)).isEqualTo(VerdictType.PASS);
    }

    @Test
    @DisplayName("CLEAN 은 검출이 하나만 있어도 불합격이다")
    void failCleanRunWithFindings() {
        assertThat(VerifyJobConfig.verdictOf(DatasetType.CLEAN, 1)).isEqualTo(VerdictType.FAIL);
    }

    /**
     * 검출이 정답 800행과 완벽히 일치해도 지금은 {@code FAIL} 이다. 개수만 보면
     * <b>오탐 400 + 누락 400 도 800</b> 이라 통과시킬 수 없다.
     * 시드도 CORRUPT run 을 {@code FAIL} 로 심는다.
     */
    @Test
    @DisplayName("CORRUPT 는 검출 수와 무관하게 불합격이다 — 집합 대조가 붙기 전까지")
    void failCorruptRunRegardlessOfCount() {
        assertThat(VerifyJobConfig.verdictOf(DatasetType.CORRUPT, 0)).isEqualTo(VerdictType.FAIL);
        assertThat(VerifyJobConfig.verdictOf(DatasetType.CORRUPT, 800)).isEqualTo(VerdictType.FAIL);
    }
}
