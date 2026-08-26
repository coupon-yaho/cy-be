package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.port.BrandDayCalendarQueryPort;
import com.kafkick.core.coupon.query.BrandDayCalendarEntry;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupontemplate.domain.CouponTemplateSchedule;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

@Service
public class BrandDayCalendarQueryService {

    private final CouponTemplateRepository couponTemplateRepository;
    private final BrandDayCalendarQueryPort calendarQueryPort;
    private final ZoneId scheduleZone;

    @Autowired
    public BrandDayCalendarQueryService(
            CouponTemplateRepository couponTemplateRepository,
            BrandDayCalendarQueryPort calendarQueryPort,
            @Value("${coupon.calendar.schedule-zone:${coupon.round-generation.schedule-zone}}")
            String scheduleZone
    ) {
        this(
                couponTemplateRepository,
                calendarQueryPort,
                ZoneId.of(scheduleZone)
        );
    }

    public BrandDayCalendarQueryService(
            CouponTemplateRepository couponTemplateRepository,
            BrandDayCalendarQueryPort calendarQueryPort,
            ZoneId scheduleZone
    ) {
        this.couponTemplateRepository = Objects.requireNonNull(
                couponTemplateRepository
        );
        this.calendarQueryPort = Objects.requireNonNull(calendarQueryPort);
        this.scheduleZone = Objects.requireNonNull(scheduleZone);
    }

    @Transactional(readOnly = true)
    public List<BrandDayCalendarEntry> findBetween(
            LocalDate from,
            LocalDate to,
            Instant asOf
    ) {
        validate(from, to, asOf);

        Instant fromInclusive = from.atStartOfDay(scheduleZone).toInstant();
        Instant toExclusive = to.plusDays(1)
                .atStartOfDay(scheduleZone)
                .toInstant();
        List<CouponRoundDetail> actualRounds =
                calendarQueryPort.findBetween(fromInclusive, toExclusive);
        Map<OccurrenceKey, CouponRoundDetail> actualByOccurrence =
                new LinkedHashMap<>();
        for (CouponRoundDetail round : actualRounds) {
            actualByOccurrence.put(
                    new OccurrenceKey(round.templateId(), round.openAt()),
                    round
            );
        }

        List<BrandDayCalendarEntry> result = new ArrayList<>();
        for (CouponTemplate template
                : couponTemplateRepository.findAllActiveByIdAsc()) {
            for (YearMonth month : monthsBetween(from, to)) {
                Instant openAt = CouponTemplateSchedule.openAt(
                        template,
                        month,
                        scheduleZone
                );
                if (openAt.isBefore(fromInclusive)
                        || !openAt.isBefore(toExclusive)) {
                    continue;
                }

                CouponRoundDetail actual = actualByOccurrence.remove(
                        new OccurrenceKey(template.id(), openAt)
                );
                result.add(actual == null
                        ? virtualEntry(template, openAt, asOf)
                        : actualEntry(actual));
            }
        }

        actualByOccurrence.values().stream()
                .map(BrandDayCalendarQueryService::actualEntry)
                .forEach(result::add);

        return result.stream()
                .sorted(Comparator
                        .comparing(BrandDayCalendarEntry::openAt)
                        .thenComparing(BrandDayCalendarEntry::templateId))
                .toList();
    }

    private BrandDayCalendarEntry virtualEntry(
            CouponTemplate template,
            Instant openAt,
            Instant asOf
    ) {
        Instant closeAt = CouponTemplateSchedule.closeAt(template, openAt);
        CouponRoundStatus status = openAt.isAfter(asOf)
                ? CouponRoundStatus.SCHEDULED
                : CouponRoundStatus.CLOSED;

        return new BrandDayCalendarEntry(
                template.id(),
                template.brandId(),
                template.name(),
                template.policyType(),
                template.discountRate(),
                template.maxDiscountAmount(),
                template.discountAmount(),
                template.eligibleGrades(),
                openAt,
                closeAt,
                status,
                null,
                null,
                null
        );
    }

    private static BrandDayCalendarEntry actualEntry(
            CouponRoundDetail round
    ) {
        return new BrandDayCalendarEntry(
                round.templateId(),
                round.brandId(),
                round.name(),
                round.policyType(),
                round.discountRate(),
                round.maxDiscountAmount(),
                round.discountAmount(),
                round.eligibleGrades(),
                round.openAt(),
                round.closeAt(),
                round.status(),
                round.couponRoundId(),
                round.totalQuantity(),
                round.totalQuantity() - round.remainingQuantity()
        );
    }

    private static List<YearMonth> monthsBetween(
            LocalDate from,
            LocalDate to
    ) {
        YearMonth first = YearMonth.from(from);
        YearMonth last = YearMonth.from(to);
        return Stream.iterate(
                        first,
                        month -> !month.isAfter(last),
                        month -> month.plusMonths(1)
                )
                .toList();
    }

    private void validate(LocalDate from, LocalDate to, Instant asOf) {
        if (from == null || to == null || asOf == null) {
            throw new BusinessException(
                    CommonErrorCode.INVALID_INPUT,
                    "달력 조회 기간과 기준 시각은 필수입니다."
            );
        }
        if (LocalDate.MAX.equals(to)) {
            throw new BusinessException(
                    CommonErrorCode.INVALID_INPUT,
                    "달력 조회 종료일이 허용 범위를 벗어났습니다."
            );
        }
        if (to.isBefore(from)) {
            throw new BusinessException(
                    CommonErrorCode.INVALID_INPUT,
                    "달력 조회 기간이 올바르지 않습니다."
            );
        }
    }

    private record OccurrenceKey(Long templateId, Instant openAt) {
    }
}
