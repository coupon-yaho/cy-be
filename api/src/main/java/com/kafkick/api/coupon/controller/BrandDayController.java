package com.kafkick.api.coupon.controller;

import java.time.LocalDate;
import java.util.List;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.dto.response.BrandDayCalendarResponse;
import com.kafkick.api.coupon.dto.response.BrandDayResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.coupon.service.BrandDayCalendarQueryService;
import com.kafkick.core.coupon.service.BrandDayQueryService;
import com.kafkick.core.support.TimeProvider;

@RestController
public class BrandDayController {

    private final BrandDayQueryService brandDayQueryService;
    private final BrandDayCalendarQueryService calendarQueryService;
    private final TimeProvider timeProvider;

    public BrandDayController(
            BrandDayQueryService brandDayQueryService,
            BrandDayCalendarQueryService calendarQueryService,
            TimeProvider timeProvider
    ) {
        this.brandDayQueryService = brandDayQueryService;
        this.calendarQueryService = calendarQueryService;
        this.timeProvider = timeProvider;
    }

    @GetMapping("/api/v1/brand-days")
    public ResponseEnvelope<List<BrandDayResponse>> findBrandDays() {
        return ResponseEnvelope.success(
                brandDayQueryService.findAll().stream()
                        .map(BrandDayResponse::from)
                        .toList()
        );
    }

    /**
     * 양끝을 포함한 브랜드 데이 달력을 조회합니다.
     * {@code from}과 {@code to}는 필수 ISO 날짜(yyyy-MM-dd)이며,
     * 누락되거나 형식이 올바르지 않으면 400 잘못된 요청으로 처리됩니다.
     */
    @GetMapping("/api/v1/calendar")
    public ResponseEnvelope<List<BrandDayCalendarResponse>> findCalendar(
            @RequestParam
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
            LocalDate from,
            @RequestParam
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
            LocalDate to
    ) {
        return ResponseEnvelope.success(
                calendarQueryService.findBetween(
                                from,
                                to,
                                timeProvider.instant()
                        ).stream()
                        .map(BrandDayCalendarResponse::from)
                        .toList()
        );
    }
}
