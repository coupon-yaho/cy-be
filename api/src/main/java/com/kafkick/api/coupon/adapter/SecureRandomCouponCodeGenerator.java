// 추측이 어려운 16자리 영문 대문자·숫자 쿠폰 코드를 생성합니다.
package com.kafkick.api.coupon.adapter;

import java.security.SecureRandom;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.port.CouponCodeGenerator;

@Component
public class SecureRandomCouponCodeGenerator implements CouponCodeGenerator {

    private static final char[] ALPHABET =
            "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".toCharArray();
    private static final int CODE_LENGTH = 16;

    private final ThreadLocal<SecureRandom> random =
            ThreadLocal.withInitial(SecureRandom::new);

    @Override
    public String generate() {
        char[] code = new char[CODE_LENGTH];
        SecureRandom currentRandom = random.get();
        for (int index = 0; index < code.length; index++) {
            code[index] = ALPHABET[currentRandom.nextInt(ALPHABET.length)];
        }
        return new String(code);
    }
}
