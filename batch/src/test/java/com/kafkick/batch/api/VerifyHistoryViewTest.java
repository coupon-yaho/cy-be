package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;

/**
 * 검증 이력 한 줄의 표현.
 *
 * <p>이 뷰의 분기 하나가 <b>판정 오독을 막는 코드</b>다 — 그런데 그것을 실행하는 테스트가
 * 없었다. 지우면 전체 스위트가 그대로 초록이라(돌연변이 생존) 다음 사람이 삼항을
 * "정리" 하는 순간 도는 중인 검증이 무결로 그려진다.
 */
class VerifyHistoryViewTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 26, 5, 0);

    @Test
    @DisplayName("판정 전에는 검출 건수를 안 준다 — 이 프로젝트에서 0 은 '무결' 의 신호값이다")
    void hidesFindingCountBeforeVerdict() {
        VerifyHistoryView running = VerifyHistoryView.of(started());

        assertThat(running.verdict()).isNull();
        assertThat(running.findingCount())
                .as("VerificationRun.start 가 0 으로 시작한다. 그대로 실으면 도는 중인 "
                        + "실행이 '검출 0건 = 합격' 으로 그려진다")
                .isNull();
        assertThat(running.findingsChecksum()).isNull();
    }

    @Test
    @DisplayName("판정이 나면 검출 건수를 그대로 준다 — 0 도 그때는 진짜 0 이다")
    void showsFindingCountAfterVerdict() {
        VerifyHistoryView passed = VerifyHistoryView.of(
                started().finish(VerdictType.PASS, 0, "checksum", "fingerprint",
                        AS_OF.plusMinutes(8)));

        assertThat(passed.findingCount()).isZero();
        assertThat(passed.findingsChecksum()).isEqualTo("checksum");
    }

    @Test
    @DisplayName("실패 판정의 800 건이 그대로 실린다 — 오염셋 대조가 이 수로 읽힌다")
    void carriesFailedFindingCount() {
        assertThat(VerifyHistoryView.of(
                started().finish(VerdictType.FAIL, 800, "c", "f", AS_OF.plusMinutes(8)))
                .findingCount()).isEqualTo(800);
    }

    @Test
    @DisplayName("창의 시작을 함께 준다 — scope 만 싣고 fromTs 를 빼면 증분 두 실행이 같아 보인다")
    void carriesWindowStart() {
        LocalDateTime fromTs = AS_OF.minusDays(1);
        VerificationRun incremental = VerificationRun.start(
                AS_OF, fromTs, ScopeType.INCREMENTAL, DatasetType.CLEAN, 1, AS_OF);

        assertThat(VerifyHistoryView.of(incremental).fromTs()).isEqualTo(fromTs);
        assertThat(VerifyHistoryView.of(started()).fromTs())
                .as("FULL 은 fromTs 가 null 이어야 한다 — VerificationRun 생성자가 강제한다")
                .isNull();
    }

    private static VerificationRun started() {
        return VerificationRun.start(AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 1, AS_OF);
    }
}
