// 판정을 게이지가 낼 수 있는 숫자로 바꿉니다.
package com.kafkick.batch.config;

import com.kafkick.core.verification.VerdictType;

/**
 * <b>{@code PASS=0}, {@code FAIL=1}.</b> 값이 둘뿐이라 성립하는 인코딩이다.
 *
 * <p>셋째 판정이 생기면 <b>규칙 파일을 안 고쳐도 알림의 뜻이 바뀐다</b> — 새 값을 2 로
 * 매핑했을 때 식이 {@code > 0} 이면 그것도 FAIL 로 울리고 {@code == 1} 이면 조용해진다.
 * 그래서 매핑을 여기 한 곳에 모으고, {@code VerdictCodeTest} 가 값이 둘인 것을 단언한다.
 * 값을 더하는 사람이 그 테스트에서 먼저 멈춘다.
 */
final class VerdictCode {

    private VerdictCode() {
    }

    static double of(VerdictType verdict) {
        if (verdict == null) {
            // 닫혔는데 판정이 없는 행. FAIL 로 접으면 가짜 알림이 뜨고, PASS 로 접으면
            // 합격으로 읽힌다. 둘 다 아니라 모름이다 — 이 저장소가 세운
            // "모르는 것과 통과한 것은 다르다" 의 세 번째 경우다.
            return Double.NaN;
        }
        return verdict == VerdictType.PASS ? 0 : 1;
    }
}
