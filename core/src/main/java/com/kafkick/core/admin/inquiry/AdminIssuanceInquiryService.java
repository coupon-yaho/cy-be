package com.kafkick.core.admin.inquiry;

import java.time.Instant;
import java.util.Objects;

import org.springframework.stereotype.Service;

import com.kafkick.core.support.TimeProvider;

/** 한 요청의 기준 시각, Mock DB 조회 행과 문의 계산을 조립합니다. */
@Service
public class AdminIssuanceInquiryService {

    private final TimeProvider timeProvider;
    private final AdminIssuanceInquirySourceReader sourceReader;
    private final IssuanceInquiryCalculator calculator;

    /** 요청 시각 공급자, 원천 조회 포트와 순수 계산기를 주입받습니다. */
    public AdminIssuanceInquiryService(
            TimeProvider timeProvider,
            AdminIssuanceInquirySourceReader sourceReader,
            IssuanceInquiryCalculator calculator
    ) {
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
        this.sourceReader = Objects.requireNonNull(sourceReader, "sourceReader");
        this.calculator = Objects.requireNonNull(calculator, "calculator");
    }

    /** 요청당 한 번 확정한 시각의 원천 행으로 회원 발급 문의를 계산합니다. */
    public AdminIssuanceInquiryResult getInquiries(AdminIssuanceInquiryQuery query) {
        Objects.requireNonNull(query, "query");
        // 세 원천이 요청 중 서로 다른 시점을 보지 않도록 관측 기준 시각을 한 번만 확정한다.
        Instant snapshotAt = timeProvider.instant();
        AdminIssuanceInquirySource source = sourceReader.create(snapshotAt);
        return calculator.calculate(source, query);
    }
}
