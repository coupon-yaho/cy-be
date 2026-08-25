package com.kafkick.core.admin.inquiry;

import java.time.Instant;
import java.util.Objects;

import org.springframework.stereotype.Service;

import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

/** 한 요청의 기준 시각, 원천 조회와 문의 계산을 조립합니다. */
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
        AdminIssuanceInquiryReadResult readResult = Objects.requireNonNull(
                sourceReader.read(query, snapshotAt), "readResult");
        return switch (readResult.availability()) {
            case AVAILABLE -> calculator.calculate(readResult.source(), query);
            case MEMBER_NOT_FOUND -> throw new BusinessException(
                    AdminIssuanceInquiryErrorCode.MEMBER_NOT_FOUND);
            case COUPON_NOT_FOUND -> throw new BusinessException(
                    AdminIssuanceInquiryErrorCode.COUPON_NOT_FOUND);
        };
    }
}
