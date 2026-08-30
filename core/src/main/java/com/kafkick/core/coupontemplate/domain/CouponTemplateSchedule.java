package com.kafkick.core.coupontemplate.domain;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

public final class CouponTemplateSchedule {

    private CouponTemplateSchedule() {
    }

    public static LocalDate occurrenceDate(
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

    public static Instant openAt(
            CouponTemplate template,
            YearMonth month,
            ZoneId scheduleZone
    ) {
        return occurrenceDate(template, month)
                .atTime(template.startTime())
                .atZone(scheduleZone)
                .toInstant();
    }

    public static Instant closeAt(
            CouponTemplate template,
            Instant openAt
    ) {
        return openAt.plus(template.durationHours(), ChronoUnit.HOURS);
    }
}
