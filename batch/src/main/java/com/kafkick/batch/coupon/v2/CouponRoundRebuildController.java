package com.kafkick.batch.coupon.v2;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 재구성의 트리거. 워밍업과 <b>URL 을 나눈다</b> — 같은 주소가 "안전한 호출" 과 "게이트를 닫는
 * 호출" 을 겸하면, 모드 인자 하나를 잘못 붙인 요청이 살아 있는 회차를 세운다. 주소가 다르면
 * 운영 문서에서도 손이 미끄러지지 않는다.
 *
 * <p>워밍업과 같은 이유로 batch 안에 있다(07 의 (a)). 포트 9091 은 compose 에서 공개하지 않는다.
 *
 * <p><b>{@code confirm} 이 사람 확인 절차다.</b> §9.7 이 "부하 중에는 돌리지 않는다. 캠페인 종료
 * 후 또는 CRITICAL 초과 시 <b>수동으로</b>" 라고 적었는데, 이 포트에는 인증이 없어 절차를
 * 지키게 만드는 코드가 이것뿐이다. 회차 번호를 한 번 더 적게 하면 <b>URL 재사용과 히스토리
 * 재실행</b>이 막힌다 — 셸 히스토리에서 화살표로 꺼낸 명령이 다른 회차를 세우는 것이 이 자리의
 * 가장 흔한 사고다.
 *
 * <p><b>부하 중 실행을 코드로 막지는 않았다.</b> {@code close_at} 이후로 제한하면 §9.7 이 함께
 * 적은 나머지 절반 — {@code CRITICAL} 초과 시 진행 중인 회차를 되살리는 것 — 까지 막힌다.
 * 그 판단은 운영자의 몫이고, 이 클래스가 지는 몫은 "실수로는 안 눌리게" 까지다.
 */
@RestController
@RequestMapping("/internal/v1/coupon-rounds")
public class CouponRoundRebuildController {

    private final CouponRoundRebuildRunner runner;

    public CouponRoundRebuildController(CouponRoundRebuildRunner runner) {
        this.runner = runner;
    }

    @PostMapping("/{couponRoundId}/rebuild")
    public ResponseEntity<CouponRoundRebuildResult> rebuild(
            @PathVariable long couponRoundId, @RequestParam long confirm) {
        if (confirm != couponRoundId) {
            // 아무것도 읽지 않고 돌아간다. 게이트를 닫기 한참 전이다.
            return ResponseEntity.badRequest().body(CouponRoundRebuildResult.rejected(
                    couponRoundId, CouponRoundRebuildStatus.CONFIRMATION_MISMATCH));
        }
        CouponRoundRebuildResult result = runner.rebuild(couponRoundId);
        return ResponseEntity.status(statusOf(result)).body(result);
    }

    /**
     * 거절을 200 으로 내보내지 않는다. 본문을 안 읽는 호출부가 <b>다시 안 세워진 회차를
     * 세워졌다고 믿는</b> 것이 가장 나쁜 결말이다. 특히 {@code OVER_ISSUED_ROUND} 는 게이트가
     * 닫힌 채 끝나므로 그 회차는 그 순간 전면 503 이다 — 이 코드가 유일한 신호다.
     */
    private static HttpStatus statusOf(CouponRoundRebuildResult result) {
        if (result.gateClosed()) {
            // **404 로 내보내면 안 된다.** "그런 회차 없다" 로 읽혀 운영자가 손을 떼는데,
            // 실제로는 그 회차가 방금 전면 503 이 됐고 다시 돌릴 때까지 그대로다.
            return HttpStatus.CONFLICT;
        }
        return switch (result.status()) {
            case REBUILT -> HttpStatus.OK;
            case CONFIRMATION_MISMATCH -> HttpStatus.BAD_REQUEST;
            case ROUND_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case REBUILD_IN_PROGRESS, STOCK_ROW_MISSING, ENGINE_NOT_V2,
                 OVER_ISSUED_ROUND -> HttpStatus.CONFLICT;
        };
    }
}
