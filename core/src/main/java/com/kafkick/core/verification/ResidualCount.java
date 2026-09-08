// 두 실행 사이에서 한 규칙의 검출이 어떻게 갈렸는지입니다.
package com.kafkick.core.verification;

/**
 * <b>개수만으로는 "그대로 남았다" 와 "다 고쳐지고 새로 생겼다" 가 같은 모양이다.</b>
 * {@code STOCK_MISMATCH 3 → 3} 을 보고 운영자가 할 일이 정반대인데, 지금까지 리포트는
 * 그 둘을 가르지 못했다 — 회차마다 절대값만 냈다.
 *
 * <p>사전예약 PRD 의 대사 보고서가 요구하는 <b>잔여 불일치</b> 축이 이것이다.
 * (나머지 셋 중 검사 건수는 CY-945 가 냈고, 정정·복구는 대사가 <b>쓰기</b>를 하는 축이라
 * 예약 도메인이 진다.)
 *
 * <p><b>셋을 더하면 합집합이다.</b> 겹치지 않는다 — 한 {@code (type, targetKey)} 는
 * 세 갈래 중 정확히 하나다. {@code uk_run_finding} 이 한 실행 안에서 같은 키를 막으므로
 * 그 성질이 성립한다.
 *
 * @param persisted 두 실행에 다 있다. <b>아무도 안 고치고 있는 것</b>이다
 * @param introduced 뒤 실행에만 있다. 그 사이에 새로 생겼다
 * @param resolved 앞 실행에만 있다. 그 사이에 사라졌다 — 고쳐졌거나, 대상이 없어졌다
 */
public record ResidualCount(int persisted, int introduced, int resolved) {

    public static final ResidualCount NONE = new ResidualCount(0, 0, 0);

    public ResidualCount {
        if (persisted < 0 || introduced < 0 || resolved < 0) {
            throw new IllegalArgumentException(
                    "잔여 집계는 음수가 될 수 없습니다. persisted=" + persisted
                            + " introduced=" + introduced + " resolved=" + resolved);
        }
    }

    /** 뒤 실행에 남아 있는 수. <b>이것이 PRD 가 말하는 "잔여 불일치 건수" 다.</b> */
    public int remaining() {
        return persisted + introduced;
    }
}
