package com.kafkick.batch.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * 확인 토큰이 <b>정말 막는지</b>만 본다. 이 포트에는 인증이 없어, 부하 중 오발을 막는 절차가
 * 이것 하나다(§9.7).
 */
class CouponRoundRebuildControllerTest {

    private static final long ROUND_ID = 700;

    private final CouponRoundRebuildRunner runner = mock(CouponRoundRebuildRunner.class);
    private final CouponRoundRebuildController controller = new CouponRoundRebuildController(runner);

    @Test
    @DisplayName("확인 토큰이 회차 번호와 다르면 러너를 부르지도 않는다 — 게이트를 닫기 한참 전이다")
    void refusesWhenConfirmationDoesNotMatch() {
        ResponseEntity<CouponRoundRebuildResult> response = controller.rebuild(ROUND_ID, 701);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().status())
                .isEqualTo(CouponRoundRebuildStatus.CONFIRMATION_MISMATCH);
        verify(runner, never()).rebuild(anyLong());
    }

    @Test
    @DisplayName("토큰이 맞으면 재구성을 돌리고 200 을 낸다")
    void runsRebuildWhenConfirmed() {
        when(runner.rebuild(ROUND_ID)).thenReturn(new CouponRoundRebuildResult(
                ROUND_ID, CouponRoundRebuildStatus.REBUILT, false, 100, 3, 2, 3, 3, 98));

        ResponseEntity<CouponRoundRebuildResult> response = controller.rebuild(ROUND_ID, ROUND_ID);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().rebuilt()).isTrue();
    }

    @Test
    @DisplayName("게이트를 건드리지 않은 거절은 원래 코드로 나간다 — 없는 회차는 404 다")
    void keepsPlainCodesForRejectionsThatLeftTheGateAlone() {
        when(runner.rebuild(ROUND_ID)).thenReturn(CouponRoundRebuildResult.rejected(
                ROUND_ID, CouponRoundRebuildStatus.ROUND_NOT_FOUND));

        assertThat(controller.rebuild(ROUND_ID, ROUND_ID).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    /**
     * 게이트를 닫은 채 끝난 거절은 <b>404 로 나가면 안 된다.</b> 404 는 "그런 회차 없다" 로
     * 읽혀 운영자가 손을 떼게 만드는데, 실제로는 그 회차가 방금 전면 503 이 됐다.
     */
    @Test
    @DisplayName("게이트가 닫힌 채 끝난 거절은 404 가 아니라 409 다")
    void reportsGateClosedRejectionsAsConflict() {
        when(runner.rebuild(ROUND_ID)).thenReturn(CouponRoundRebuildResult.rejectedAfterClose(
                ROUND_ID, CouponRoundRebuildStatus.ROUND_NOT_FOUND));

        assertThat(controller.rebuild(ROUND_ID, ROUND_ID).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(controller.rebuild(ROUND_ID, ROUND_ID).getBody().gateClosed()).isTrue();
    }

    @Test
    @DisplayName("초과 발급 거절도 200 으로 나가지 않는다")
    void doesNotReportRejectionsAsSuccess() {
        when(runner.rebuild(ROUND_ID)).thenReturn(CouponRoundRebuildResult.rejected(
                ROUND_ID, CouponRoundRebuildStatus.OVER_ISSUED_ROUND));

        assertThat(controller.rebuild(ROUND_ID, ROUND_ID).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
    }
}
