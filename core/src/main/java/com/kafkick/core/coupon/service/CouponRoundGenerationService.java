package com.kafkick.core.coupon.service;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupon.exception.CouponRoundAlreadyExistsException;
import com.kafkick.core.coupon.service.result.CouponRoundGenerationResult;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;

@Service
public class CouponRoundGenerationService {

    private final CouponTemplateRepository couponTemplateRepository;
    private final CouponRoundCreationService couponRoundCreationService;
    private final ZoneId scheduleZone;
    private final int maxGenerationDays;

    @Autowired
    public CouponRoundGenerationService(
            CouponTemplateRepository couponTemplateRepository,
            CouponRoundCreationService couponRoundCreationService,
            @Value("${coupon.round-generation.schedule-zone:Asia/Seoul}")
            String scheduleZone,
            @Value("${coupon.round-generation.max-days:30}")
            int maxGenerationDays
    ) {
        this(
                couponTemplateRepository,
                couponRoundCreationService,
                ZoneId.of(scheduleZone),
                maxGenerationDays
        );
    }

    public CouponRoundGenerationService(
            CouponTemplateRepository couponTemplateRepository,
            CouponRoundCreationService couponRoundCreationService,
            ZoneId scheduleZone,
            int maxGenerationDays
    ) {
        this.couponTemplateRepository = Objects.requireNonNull(
                couponTemplateRepository
        );
        this.couponRoundCreationService = Objects.requireNonNull(
                couponRoundCreationService
        );
        this.scheduleZone = Objects.requireNonNull(scheduleZone);
        if (maxGenerationDays <= 0) {
            throw new IllegalArgumentException(
                    "최대 회차 생성 기간은 0보다 커야 합니다."
            );
        }
        this.maxGenerationDays = maxGenerationDays;
    }

    public CouponRoundGenerationResult generate(
            LocalDate fromDate,
            LocalDate toDate,
            Instant generatedAt
    ) {
        validateRange(fromDate, toDate, generatedAt);

        int creationTargets = 0;
        int createdCount = 0;
        int duplicateCount = 0;
        List<CouponTemplate> activeTemplates =
                couponTemplateRepository.findAllActiveByIdAsc();

        for (CouponTemplate template : activeTemplates) {
            for (YearMonth month : monthsBetween(fromDate, toDate)) {
                LocalDate occurrenceDate = occurrenceDate(template, month);
                if (occurrenceDate.isBefore(fromDate)
                        || occurrenceDate.isAfter(toDate)) {
                    continue;
                }

                creationTargets++;
                Instant openAt = occurrenceDate
                        .atTime(template.startTime())
                        .atZone(scheduleZone)
                        .toInstant();
                CouponRound couponRound = CouponRound.schedule(
                        template,
                        openAt,
                        generatedAt
                );
                CouponStock initialStock = CouponStock.initialize(
                        template.stockPerOccurrence(),
                        generatedAt
                );

                try {
                    couponRoundCreationService.create(
                            couponRound,
                            initialStock
                    );
                    createdCount++;
                } catch (CouponRoundAlreadyExistsException exception) {
                    duplicateCount++;
                }
            }
        }

        return new CouponRoundGenerationResult(
                creationTargets,
                createdCount,
                duplicateCount
        );
    }

    private static List<YearMonth> monthsBetween(
            LocalDate fromDate,
            LocalDate toDate
    ) {
        YearMonth firstMonth = YearMonth.from(fromDate);
        YearMonth lastMonth = YearMonth.from(toDate);

        return Stream.iterate(
                        firstMonth,
                        month -> !month.isAfter(lastMonth),
                        month -> month.plusMonths(1)
                )
                .toList();
    }

    private static LocalDate occurrenceDate(
            CouponTemplate template,
            YearMonth month
    ) {
        DayOfWeek dayOfWeek = switch (template.dayOfWeek()) {
            case MON -> DayOfWeek.MONDAY;
            case TUE -> DayOfWeek.TUESDAY;
            case WED -> DayOfWeek.WEDNESDAY;
            case THU -> DayOfWeek.THURSDAY;
            case FRI -> DayOfWeek.FRIDAY;
            case SAT -> DayOfWeek.SATURDAY;
            case SUN -> DayOfWeek.SUNDAY;
        };

        return month.atDay(1).with(
                TemporalAdjusters.dayOfWeekInMonth(
                        template.nthWeek(),
                        dayOfWeek
                )
        );
    }

    private void validateRange(
            LocalDate fromDate,
            LocalDate toDate,
            Instant generatedAt
    ) {
        if (fromDate == null || toDate == null || generatedAt == null) {
            throw new IllegalArgumentException(
                    "회차 생성 기간과 생성 시각은 필수입니다."
            );
        }
        if (toDate.isBefore(fromDate)) {
            throw new IllegalArgumentException(
                    "회차 생성 종료일은 시작일보다 빠를 수 없습니다."
            );
        }
        if (ChronoUnit.DAYS.between(fromDate, toDate)
                > maxGenerationDays) {
            throw new IllegalArgumentException(
                    "회차 생성 기간이 허용 범위를 초과했습니다."
            );
        }
    }
}
