// 판정 규칙을 값 단위로 고정합니다. 잡을 안 띄우고 분기만 봅니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.verification.VerdictType;

/**
 * <b>CLEAN 판정만 본다.</b> 오염셋 판정은 정답 매니페스트와의 집합 일치라
 * {@code VerifyJobManifestTest} 가 잡 전체를 돌려 확인한다 —
 * 값 하나로 고정할 수 있는 규칙이 아니다.
 */
class VerdictRuleTest {

    @Test
    @DisplayName("CLEAN 은 검출 0건이 합격이다")
    void passCleanRunWithoutFindings() {
        assertThat(VerifyJobConfig.verdictOfClean(0)).isEqualTo(VerdictType.PASS);
    }

    @Test
    @DisplayName("CLEAN 은 검출이 하나만 있어도 불합격이다")
    void failCleanRunWithFindings() {
        assertThat(VerifyJobConfig.verdictOfClean(1)).isEqualTo(VerdictType.FAIL);
    }
}
