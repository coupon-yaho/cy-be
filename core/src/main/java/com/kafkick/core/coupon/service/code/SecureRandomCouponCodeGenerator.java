package com.kafkick.core.coupon.service.code;

import java.security.SecureRandom;

import org.springframework.stereotype.Component;

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
