package com.kafkick.batch.api;

import java.util.List;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.core.batch.BatchRunRepository;
import com.kafkick.core.support.response.ResponseEnvelope;

/**
 * 배치 실행 이력. 세 잡(expire·verify·cleanup)을 함께 준다.
 *
 * <p>검증 이력과 가른 이유는 출처가 달라서다. 이쪽은 Spring Batch 메타의 실행 기록이고
 * 저쪽은 판정 기록이다 — verifyJob 은 두 곳에 다 있지만, 가드에 걸려 죽으면 실행만 남고
 * 검증 행은 안 생긴다.
 *
 * <p>회차 상태 전이는 여기 안 나온다. {@code CouponRoundScheduler} 는 Spring Batch 잡이
 * 아니라 {@code @Scheduled} 라 메타에 행이 안 남는다 — 그 축은 지표로 본다.
 */
@RestController
@RequestMapping("/api/v1/admin/batch")
public class BatchHistoryController {

    private final BatchRunRepository runs;

    public BatchHistoryController(BatchRunRepository runs) {
        this.runs = runs;
    }

    /** 최근 실행부터 한 페이지. {@code jobName} 을 안 주면 세 잡을 다 준다. */
    @GetMapping("/runs")
    @Transactional(readOnly = true, timeoutString = "${batch.admin.timeout-seconds:5}")
    public ResponseEnvelope<HistoryPage<BatchRunView>> history(
            @RequestParam(required = false) String jobName,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        int size = HistoryPage.pageSize(limit);
        int from = HistoryPage.pageOffset(offset);
        List<BatchRunView> items = runs.findRecent(jobName, size, from).stream()
                .map(BatchRunView::of)
                .toList();
        return ResponseEnvelope.success(
                new HistoryPage<>(items, runs.countRecent(jobName), size, from));
    }
}
