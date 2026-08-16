// V4 라이터의 상한 가드가 어댑터 다섯 규칙과 같은 규약을 지키는지 확인합니다.
package com.kafkick.batch.replay;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.verification.VerificationFindingRepository;

/**
 * <b>어댑터 쪽 {@code rejectNonPositiveLimit} 과 대칭입니다.</b> 규칙은 여섯인데 상한 가드가
 * 사는 곳이 둘로 갈려 있습니다 — 다섯은 {@code VerificationRuleJdbcAdapter#requireLimit},
 * V4 만 이 라이터의 생성자입니다. V4 는 청크 라이터라 규칙 Step 형태가 아니기 때문입니다.
 *
 * <p>그래서 <b>어댑터 테스트가 여섯을 다 돌 수 없습니다</b> — storage 모듈은 batch 를 못 봅니다.
 * 실제로 그쪽 테스트가 "여섯 규칙" 이라고 이름 붙이고 다섯만 돌고 있었고, 빠진 것이 V4 였습니다.
 * 두 테스트가 짝으로 있어야 여섯이 채워집니다.
 *
 * <p>가드가 없으면 {@code LIMIT 0} 이 에러가 아니라 <b>0행</b>이라, 규칙이 조용히 아무것도
 * 안 잡고 잡이 성공으로 끝납니다. 정상셋 0건이 합격 조건이라 그 침묵은 성공과 구분되지 않습니다.
 */
class IllegalTransitionItemWriterTest {

    /** 저장소 관행대로 손으로 만든다. 상호작용을 검증하지 않으므로 빈 구현이면 충분하다. */
    private final VerificationFindingRepository findings = (runId, detected) -> {
    };

    @Test
    @DisplayName("V4 라이터도 상한 0 과 음수를 거부한다 — 어댑터 다섯 규칙과 같은 규약이다")
    void rejectNonPositiveLimit() {
        assertThatThrownBy(() -> new IllegalTransitionItemWriter(findings, 1L, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("검출 상한은 1 이상");

        assertThatThrownBy(() -> new IllegalTransitionItemWriter(findings, 1L, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("검출 상한은 1 이상");
    }

    /**
     * 상한 1 은 유효하다. 하한만 막고 상한은 안 막는 것이 어댑터와 다른 점인데
     * ({@code ruleStep} 은 {@code Integer.MAX_VALUE} 도 막는다) 그쪽만 {@code limit + 1} 을
     * 요청해 넘칠 수 있기 때문이다 — 라이터는 {@code +1} 이 없다.
     */
    @Test
    @DisplayName("상한 1 과 Integer.MAX_VALUE 는 받는다 — 라이터는 +1 을 안 해 넘칠 일이 없다")
    void acceptPositiveLimit() {
        assertThatCode(() -> new IllegalTransitionItemWriter(findings, 1L, 1))
                .doesNotThrowAnyException();
        assertThatCode(() -> new IllegalTransitionItemWriter(findings, 1L, Integer.MAX_VALUE))
                .doesNotThrowAnyException();
    }
}
