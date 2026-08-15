// 한 실행이 볼 이력을 통째로 얼린 경계입니다. 결정론이 여기서 나옵니다.
package com.kafkick.core.verification.replay;

import java.time.LocalDateTime;

/**
 * <b>{@code created_at <= asOf} 만으로는 실행이 볼 행이 고정되지 않습니다.</b>
 * 청크마다 트랜잭션이 커밋되므로 창마다 새 스냅샷을 보고, 그 사이 커밋된 행은
 * 창 위치에 따라 들어가기도 하고 빠지기도 합니다. 같은 asOf 를 두 번 돌려도 결과가 달라집니다.
 *
 * <p>그래서 {@code maxHistoryId} 로 상한을 겁니다. 실행 시작에 한 번 재고 끝까지 씁니다.
 * {@code dataset_fingerprint} 의 첫 재료와 같은 값이라 따로 스캔하지 않습니다.
 *
 * <p><b>완전한 스냅샷은 아닙니다.</b> 우리가 최대값을 잰 뒤에 커밋되는, 그보다 작은 식별자가
 * 있을 수 있습니다(먼저 시작해 늦게 커밋한 트랜잭션). 창을 좁히는 것이지 닫는 것이 아닙니다.
 * 완전히 닫으려면 실행 전체가 한 트랜잭션이어야 하는데, 그러면 청크 재시작을 잃습니다.
 *
 * @param minIssuanceId asOf 이하 이력을 가진 가장 작은 발급건
 * @param maxIssuanceId 같은 조건의 가장 큰 발급건
 * @param maxHistoryId  asOf 이하 이력 중 가장 큰 이력 식별자. 이 실행의 상한
 * @param latestCreatedAt 필터와 무관한 <b>전체</b> 이력의 마지막 시각. asOf 검증에 쓴다
 */
public record ReplayScanRange(
        long minIssuanceId,
        long maxIssuanceId,
        long maxHistoryId,
        LocalDateTime latestCreatedAt
) {

    public ReplayScanRange {
        if (minIssuanceId > maxIssuanceId) {
            throw new IllegalArgumentException(
                    "발급건 구간이 뒤집혔습니다. min=" + minIssuanceId + " max=" + maxIssuanceId);
        }
        if (latestCreatedAt == null) {
            throw new IllegalArgumentException("마지막 이력 시각이 필요합니다.");
        }
    }

    /**
     * asOf 가 이 데이터의 마지막 이력보다 앞서는가.
     *
     * <p>앞서면 이력만 잘리고 {@code issuances.status} 는 현재값 그대로다.
     * 정상 데이터에서도 리플레이 결과와 현재 상태가 어긋나는 것이 당연해진다.
     */
    public boolean isBefore(LocalDateTime asOf) {
        return asOf.isBefore(latestCreatedAt);
    }
}
