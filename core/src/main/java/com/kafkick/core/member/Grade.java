package com.kafkick.core.member;

public enum Grade {

    WELCOME(1),
    SILVER(2),
    GOLD(4),
    VIP(8);

    private final int bitValue;

    Grade(int bitValue) {
        this.bitValue = bitValue;
    }

    public int getBitValue() {
        return bitValue;
    }
}
