package com.kafkick.api.coupon.adapter;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SecureRandomCouponCodeGeneratorTest {

    @Test
    @DisplayName("16자리 영문 대문자와 숫자로 서로 다른 쿠폰 코드를 생성한다")
    void generateCouponCode() {
        SecureRandomCouponCodeGenerator generator =
                new SecureRandomCouponCodeGenerator();
        Set<String> codes = new HashSet<>();

        for (int count = 0; count < 100; count++) {
            codes.add(generator.generate());
        }

        assertThat(codes).hasSize(100);
        assertThat(codes).allMatch(code -> code.matches(
                "[A-HJ-NP-Z2-9]{16}"
        ));
    }
}
