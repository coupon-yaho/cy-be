// 만료 대기 건수를 막힌 몫과 나머지로 가른 값입니다.
package com.kafkick.core.expiration;

/**
 * 만료 대기 건수를 막힌 몫과 나머지로 가른 값.
 *
 * <p><b>한 문장에서 함께 센다.</b> 둘을 따로 세면 그 사이에 값이 움직여
 * {@code total < blocked} 같은 조합이 나온다.
 *
 * @param total   기한이 지났는데 아직 {@code ISSUED} 인 전체
 * @param blocked 그중 이번 실행이 건너뛴 회차의 몫
 */
public record PendingExpiration(long total, long blocked) {

    /**
     * 배치가 처리했어야 하는데 안 된 몫. 이 값이 0 이 아니면 <b>서버를 봐야 한다.</b>
     *
     * <p><b>알림 식이 이 뺄셈을 하지 않게 하려고 여기서 한다.</b> 게이지 둘을 내보내고
     * 관제가 빼면, 값을 따로 set 하는 사이에 스크레이프가 끼어 한쪽만 새 값인 샘플이
     * 나온다. 한 문장에서 나온 값을 여기서 빼서 <b>한 시계열로</b> 내보내면 그 틈이 없다.
     */
    public long unexplained() {
        return total - blocked;
    }
}
