package com.kafkick.core.admin.issuancehistory;

import java.time.Instant;
import java.util.Objects;

import org.springframework.stereotype.Service;

import com.kafkick.core.admin.issuancehistory.mock.AdminIssuanceHistoryMockDataFactory;
import com.kafkick.core.support.TimeProvider;

/** 관리자 발급 상태 변경 이력의 요청 기준 시각, 원천 조회와 계산을 조립합니다. */
@Service
public class AdminIssuanceHistoryService {

    private final TimeProvider timeProvider;
    private final AdminIssuanceHistoryMockDataFactory mockDataFactory;
    private final IssuanceHistoryCalculator calculator;

    /**
     * 요청 시간, 원시 이력 Factory와 순수 계산기를 주입받습니다.
     *
     * @param timeProvider 요청 전체가 공유할 기준 시각 공급자
     * @param mockDataFactory Repository 대용 원시 이력 Factory
     * @param calculator 원시 이력을 화면 결과로 변환할 계산기
     */
    public AdminIssuanceHistoryService(
            TimeProvider timeProvider,
            AdminIssuanceHistoryMockDataFactory mockDataFactory,
            IssuanceHistoryCalculator calculator
    ) {
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
        this.mockDataFactory = Objects.requireNonNull(mockDataFactory, "mockDataFactory");
        this.calculator = Objects.requireNonNull(calculator, "calculator");
    }

    /**
     * 요청 필터에 맞는 발급 상태 변경 이력 페이지와 요약을 반환합니다.
     *
     * @param query 업무 필터와 Keyset 페이지 조건
     * @return 계산기가 만든 이력 조회 결과
     */
    public AdminIssuanceHistoryResult getHistories(AdminIssuanceHistoryQuery query) {
        Objects.requireNonNull(query, "query");
        // 한 요청의 원천과 계산이 같은 시각을 보도록 기준 시각을 단 한 번 확정합니다.
        Instant snapshotAt = timeProvider.instant();
        AdminIssuanceHistorySource source = mockDataFactory.create(snapshotAt);
        return calculator.calculate(source, query);
    }
}
