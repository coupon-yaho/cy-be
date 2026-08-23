package com.kafkick.core.admin.issuancehistory;

import java.util.Objects;

import org.springframework.stereotype.Component;

/** 관리자 응답에서 발급 코드의 가운데 여덟 자리를 가립니다. */
@Component
public class IssuanceCodeMasker {

    /** 정확히 16자인 발급 코드를 앞뒤 네 자리만 남긴 표현으로 변환합니다. */
    public String mask(String issuanceCode) {
        Objects.requireNonNull(issuanceCode, "issuanceCode");
        if (issuanceCode.length() != 16) {
            throw new IllegalArgumentException("issuanceCode는 정확히 16자여야 합니다.");
        }
        return issuanceCode.substring(0, 4) + "********" + issuanceCode.substring(12);
    }
}
