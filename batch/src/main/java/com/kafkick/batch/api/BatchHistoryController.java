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

    /**
     * 최근 실행부터 한 페이지. {@code jobName} 을 안 주면 세 잡을 다 준다.
     *
     * <p><b>이 목록은 스냅샷이고 OFFSET 페이지네이션이다.</b> id 내림차순이라, 페이지 요청 사이에
     * 새 실행이 생기면 경계가 밀려 <b>같은 행이 다시 나오고 뒤쪽 행이 빠진다</b> — 여러 페이지를
     * 이어 붙여 집계하는 소비자는 틀린 수를 얻는다. 봇 리뷰가 짚은 그대로다.
     *
     * <p><b>그런데 이 화면에서는 도달하지 않는다(실측).</b> 세 잡이 전부 <b>일 1회</b>이고
     * (expire 04:10 · cleanup 04:30 · verify 05:00 UTC) 배치 메타를
     * {@code batch.cleanup.metadata-keep-days}(30일)가 걷으므로 <b>전체가 약 90행</b>이다.
     * {@code HistoryPage.MAX_LIMIT} 이 200 이라 <b>한 요청에 다 들어오고</b>, 응답의
     * {@code total} 로 다 받았는지 확인할 수 있다. 그래서 커서로 바꾸지 않는다 — 안 쓰는
     * 경로를 위해 API 표면을 늘리지 않는다.
     *
     * <p>⚠️ <b>다시 볼 기준</b> — 보존 창을 늘리거나 어느 잡의 주기를 하루보다 잦게 바꾸면
     * 전체가 200행을 넘고 그때부터 위 사고가 실재한다. 그날 첫 응답의 마지막
     * {@code executionId} 를 커서로 받아 {@code id <= :anchor} 로 범위를 얼리고,
     * {@code total} 도 같은 조건으로 센다.
     */
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
