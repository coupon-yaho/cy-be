package com.kafkick.batch.coupon.v2;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 워밍업의 트리거. batch 안에 두면 <b>프로세스 간</b> 겹침이 사라진다(07 의 (a)) — batch 는 1대다.
 * api 는 여러 대라 같은 엔드포인트를 거기 두면 한쪽이 게이트를 연 뒤 다른 쪽이 카운터를
 * 덮어쓰는 창이 열린다.
 *
 * <p><b>다만 여기는 HTTP 라 워커 스레드가 여럿이다.</b> 프로세스 안의 겹침까지 토폴로지가
 * 막아 주지는 않으므로, 그 몫은 {@link CouponRoundWarmupRunner} 의 회차 단위 가드가 진다.
 *
 * <p>스케줄러가 아닌 이유는 이 단위의 범위다. 주기 스캔은 "이미 열린 회차를 다시 만나는"
 * 경우를 반드시 만들고, 그 경우의 정답(게이트를 닫고 재구성)은 S8b 다. 측정 시나리오도
 * 회차 오픈 전 한 번이면 된다(설계 §10.2).
 *
 * <p>포트 9091 은 compose 에서 공개하지 않는다. {@code /api/v1/admin/**} 이 공개 8080 에
 * 무인증으로 있는 것과 여기가 다른 점이다 — 재고를 통째로 쓰는 연산을 그쪽에 두지 않았다.
 */
@RestController
@RequestMapping("/internal/v1/coupon-rounds")
public class CouponRoundWarmupController {

    private final CouponRoundWarmupRunner runner;

    public CouponRoundWarmupController(CouponRoundWarmupRunner runner) {
        this.runner = runner;
    }

    @PostMapping("/{couponRoundId}/warmup")
    public ResponseEntity<CouponRoundWarmupResult> warmUp(@PathVariable long couponRoundId) {
        CouponRoundWarmupResult result = runner.warmUp(couponRoundId);
        return ResponseEntity.status(statusOf(result)).body(result);
    }

    /**
     * 거절을 200 으로 내보내지 않는다. 부하 스크립트가 회차마다 이걸 보고 다음 단계를 정하므로,
     * 본문을 안 읽는 호출부가 <b>안 올라간 회차를 올라갔다고 믿는</b> 것이 가장 나쁜 결말이다.
     */
    private static HttpStatus statusOf(CouponRoundWarmupResult result) {
        return switch (result.status()) {
            case WARMED -> HttpStatus.OK;
            case ROUND_NOT_FOUND -> HttpStatus.NOT_FOUND;
            // 나머지는 전부 "이 회차는 지금 워밍업 대상이 아니다" 라 409 다.
            case GATE_ALREADY_OPEN, WARMUP_IN_PROGRESS, STOCK_ROW_MISSING, ENGINE_NOT_V2,
                 ROUND_ALREADY_OPENED, OVER_ISSUED_ROUND -> HttpStatus.CONFLICT;
        };
    }
}
