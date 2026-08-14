// 멤버십 등급과 DB 저장용 비트마스크 변환 기능을 제공합니다.
package com.kafkick.core.coupon.domain;

import java.util.EnumSet;
import java.util.Collections;
import java.util.Set;

public enum MembershipGrade {

    WELCOME(1),
    SILVER(2),
    GOLD(4),
    VIP(8);

    private static final int SUPPORTED_MASK =
            WELCOME.bitValue
                    | SILVER.bitValue
                    | GOLD.bitValue
                    | VIP.bitValue;

    private final int bitValue;

    MembershipGrade(int bitValue) {
        this.bitValue = bitValue;
    }

    public int getBitValue() {
        return bitValue;
    }

    public static int toMask(Set<MembershipGrade> grades) {
        if (grades == null || grades.isEmpty()) {
            throw new IllegalArgumentException(
                    "멤버십 등급이 필요합니다."
            );
        }

        return grades.stream()
                .mapToInt(MembershipGrade::getBitValue)
                .reduce(0, (left, right) -> left | right);
    }

    public static Set<MembershipGrade> fromMask(int mask) {
        if (mask <= 0 || (mask & ~SUPPORTED_MASK) != 0) {
            throw new IllegalArgumentException(
                    "올바르지 않은 멤버십 등급 마스크입니다."
            );
        }

        EnumSet<MembershipGrade> grades =
                EnumSet.noneOf(MembershipGrade.class);

        for (MembershipGrade grade : values()) {
            if ((mask & grade.bitValue) != 0) {
                grades.add(grade);
            }
        }

        return Collections.unmodifiableSet(grades);
    }
}
