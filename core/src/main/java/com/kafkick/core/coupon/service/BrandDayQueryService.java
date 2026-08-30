package com.kafkick.core.coupon.service;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.query.BrandDaySchedule;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;

@Service
public class BrandDayQueryService {

    private final CouponTemplateRepository couponTemplateRepository;

    public BrandDayQueryService(
            CouponTemplateRepository couponTemplateRepository
    ) {
        this.couponTemplateRepository = Objects.requireNonNull(
                couponTemplateRepository
        );
    }

    @Transactional(readOnly = true)
    public List<BrandDaySchedule> findAll() {
        return couponTemplateRepository.findAllActiveByIdAsc().stream()
                .map(BrandDayQueryService::toSchedule)
                .sorted(Comparator
                        .comparingInt(BrandDaySchedule::nthWeek)
                        .thenComparing(BrandDaySchedule::dayOfWeek)
                        .thenComparing(BrandDaySchedule::startTime)
                        .thenComparing(BrandDaySchedule::templateId))
                .toList();
    }

    private static BrandDaySchedule toSchedule(CouponTemplate template) {
        return new BrandDaySchedule(
                template.id(),
                template.brandId(),
                template.name(),
                template.nthWeek(),
                template.dayOfWeek(),
                template.startTime(),
                template.durationHours(),
                template.eligibleGrades()
        );
    }
}
