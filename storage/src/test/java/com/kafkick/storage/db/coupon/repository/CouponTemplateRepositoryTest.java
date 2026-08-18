// 실제 MySQL과 Flyway 스키마에서 쿠폰 템플릿 저장 및 DB 제약 변환을 검증합니다.
package com.kafkick.storage.db.coupon.repository;

import java.time.LocalTime;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupon.port.CouponTemplatePage;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.db.RepositoryTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RepositoryTest
@Import(CouponTemplateRepositoryImpl.class)
class CouponTemplateRepositoryTest {

    @Autowired
    private CouponTemplateRepositoryImpl couponTemplateRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void insertBrand() {
        jdbcTemplate.update(
                "INSERT INTO brands (id, name, category) VALUES (?, ?, ?)",
                1L,
                "테스트 브랜드",
                "카페"
        );
    }

    @Test
    @DisplayName("쿠폰 템플릿을 MySQL에 저장하고 DB 값을 도메인 모델로 반환한다")
    void saveCouponTemplate() {
        CouponTemplate couponTemplate = createCouponTemplate(1L);

        CouponTemplate savedCouponTemplate =
                couponTemplateRepository.save(couponTemplate);

        assertThat(savedCouponTemplate.id()).isPositive();
        assertThat(savedCouponTemplate.brandId()).isEqualTo(1L);
        assertThat(savedCouponTemplate.policyType())
                .isEqualTo(CouponPolicyType.FIXED_AMOUNT);
        assertThat(savedCouponTemplate.discountAmount()).isEqualTo(5_000);
        assertThat(savedCouponTemplate.eligibleGrades())
                .containsExactly(
                        MembershipGrade.GOLD,
                        MembershipGrade.VIP
                );
    }

    @Test
    @DisplayName("정률 상한 쿠폰 템플릿을 MySQL에 저장한다")
    void savePercentCappedCouponTemplate() {
        CouponTemplate couponTemplate = CouponTemplate.create(
                1L,
                "골드 VIP 20% 할인",
                CouponPolicyType.PERCENT_CAPPED,
                20,
                10_000,
                null,
                7,
                2,
                CouponDayOfWeek.WED,
                LocalTime.of(10, 0),
                2,
                100,
                Set.of(
                        MembershipGrade.GOLD,
                        MembershipGrade.VIP
                )
        );

        CouponTemplate savedCouponTemplate =
                couponTemplateRepository.save(couponTemplate);

        assertThat(savedCouponTemplate.id()).isPositive();
        assertThat(savedCouponTemplate.policyType())
                .isEqualTo(CouponPolicyType.PERCENT_CAPPED);
        assertThat(savedCouponTemplate.discountRate()).isEqualTo(20);
        assertThat(savedCouponTemplate.maxDiscountAmount())
                .isEqualTo(10_000);
        assertThat(savedCouponTemplate.discountAmount()).isNull();
    }

    @Test
    @DisplayName("존재하지 않는 브랜드이면 DB 제약 위반을 COUPON-101로 변환한다")
    void rejectMissingBrand() {
        CouponTemplate couponTemplate = createCouponTemplate(999L);

        assertThatThrownBy(
                () -> couponTemplateRepository.save(couponTemplate)
        )
                .isInstanceOf(BusinessException.class)
                .satisfies(exception -> {
                    BusinessException businessException =
                            (BusinessException) exception;

                    assertThat(businessException.getErrorCode())
                            .isEqualTo(
                                    CouponTemplateErrorCode.INVALID_COUPON_TEMPLATE
                            );
                });
    }

    @Test
    @DisplayName("저장한 쿠폰 템플릿을 ID로 단건 조회한다")
    void findCouponTemplateById() {
        CouponTemplate savedCouponTemplate =
                couponTemplateRepository.save(createCouponTemplate(1L));

        CouponTemplate foundCouponTemplate = couponTemplateRepository
                .findById(savedCouponTemplate.id())
                .orElseThrow();

        assertThat(foundCouponTemplate.id())
                .isEqualTo(savedCouponTemplate.id());
        assertThat(foundCouponTemplate.brandId()).isEqualTo(1L);
        assertThat(foundCouponTemplate.name())
                .isEqualTo(savedCouponTemplate.name());
        assertThat(foundCouponTemplate.eligibleGrades())
                .containsExactly(
                        MembershipGrade.GOLD,
                        MembershipGrade.VIP
                );
    }

    @Test
    @DisplayName("쿠폰 템플릿 페이지를 ID 오름차순으로 조회한다")
    void findCouponTemplatePageByIdAsc() {
        CouponTemplate firstCouponTemplate =
                couponTemplateRepository.save(createCouponTemplate(1L));
        CouponTemplate secondCouponTemplate =
                couponTemplateRepository.save(createCouponTemplate(1L));

        CouponTemplatePage firstPage =
                couponTemplateRepository.findPageByIdAsc(0, 1);
        CouponTemplatePage secondPage =
                couponTemplateRepository.findPageByIdAsc(1, 1);

        assertThat(firstPage.content())
                .extracting(CouponTemplate::id)
                .containsExactly(firstCouponTemplate.id());
        assertThat(firstPage.page()).isZero();
        assertThat(firstPage.size()).isEqualTo(1);
        assertThat(firstPage.totalElements()).isEqualTo(2);
        assertThat(firstPage.totalPages()).isEqualTo(2);

        assertThat(secondPage.content())
                .extracting(CouponTemplate::id)
                .containsExactly(secondCouponTemplate.id());
        assertThat(secondPage.page()).isEqualTo(1);
        assertThat(secondPage.size()).isEqualTo(1);
        assertThat(secondPage.totalElements()).isEqualTo(2);
        assertThat(secondPage.totalPages()).isEqualTo(2);
    }

    private CouponTemplate createCouponTemplate(Long brandId) {
        return CouponTemplate.create(
                brandId,
                "골드 VIP 5천원 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                30,
                1,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of(
                        MembershipGrade.GOLD,
                        MembershipGrade.VIP
                )
        );
    }
}
