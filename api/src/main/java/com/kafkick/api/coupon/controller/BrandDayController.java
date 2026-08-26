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
