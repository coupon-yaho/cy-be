// 검증 대상 데이터셋입니다. 물리적으로 분리된 스키마를 가리킵니다.
package com.kafkick.core.verification;

/**
 * CORRUPT 스키마에는 uk_coupon_member · uk_coupon_code · ck_stock_range 를 걸지 않습니다.
 * 오염 유형 5·6·1·3 이 바로 그 제약을 위반해야 데이터가 들어가기 때문입니다.
 * 이 셋이 CLEAN 에만 있다는 사실 자체가 "불변식은 DB 제약으로 표현한다"의 증거입니다.
 */
public enum DatasetType {

    /** 정상셋. 기대값은 0건 */
    CLEAN,

    /** 오염셋. 주입 700건에 정답 800행 */
    CORRUPT
}
