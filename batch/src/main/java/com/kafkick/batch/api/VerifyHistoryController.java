package com.kafkick.batch.api;

import java.util.List;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.core.support.response.ResponseEnvelope;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.VerificationRunRepository;

/**
 * 검증 실행 이력. 관제 화면의 히스토리 표가 쓴다.
 *
 * <p>기존 runs 리소스의 컬렉션이라 verify 아래 둔다. 단건은 VerifyTriggerController 가
 * {@code /runs/&#123;executionId&#125;} 로 준다.
 *
 * <p>집계 엔드포인트는 두지 않는다. 검증은 하루 몇 건이라 전체가 수십 건이고, 화면이 한 번
 * 받아 직접 집계하면 된다. total 을 함께 주는 이유가 그것이다.
 */
@RestController
@RequestMapping("/api/v1/admin/verify")
public class VerifyHistoryController {

    private final VerificationRunRepository runs;

    public VerifyHistoryController(VerificationRunRepository runs) {
        this.runs = runs;
    }

    /**
     * 최근 실행부터 한 페이지. {@code dataset} 을 안 주면 전체를 준다.
     *
     * <p>목록과 건수를 한 트랜잭션으로 묶는다. 따로 읽으면 그 사이에 새 실행이 들어와
     * 화면이 마지막 페이지를 잘못 계산한다.
     */
    @GetMapping("/runs")
    @Transactional(readOnly = true, timeoutString = "${batch.admin.timeout-seconds:5}")
    public ResponseEnvelope<HistoryPage<VerifyHistoryView>> history(
            @RequestParam(required = false) DatasetType dataset,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        int size = HistoryPage.pageSize(limit);
        int from = HistoryPage.pageOffset(offset);
        List<VerifyHistoryView> items = runs.findRecent(dataset, size, from).stream()
                .map(VerifyHistoryView::of)
                .toList();
        return ResponseEnvelope.success(
                new HistoryPage<>(items, runs.countRecent(dataset), size, from));
    }
}
