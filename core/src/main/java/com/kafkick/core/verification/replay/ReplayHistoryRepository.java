// 리플레이 입력을 읽는 계약입니다. issuance_histories 를 아는 곳은 어댑터 하나뿐입니다.
package com.kafkick.core.verification.replay;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * <b>발급건 식별자 구간으로 자릅니다. 이력 식별자로 자르지 않습니다.</b>
 * 이력 식별자로 자르면 한 발급건의 이력이 두 청크로 쪼개져 앞 조각만 접게 되고,
 * 상태가 중간값으로 굳습니다.
 *
 * <p>{@code issuance_histories} 에는 {@code issuance_id} 단일 인덱스(FK)뿐이라
 * 전체를 한 번에 정렬하면 534만 행 filesort 입니다. 구간으로 잘라야 인덱스 범위 접근이 되고
 * 정렬도 구간 안에서만 일어납니다.
 */
public interface ReplayHistoryRepository {

    /**
     * 이 실행이 볼 이력의 경계를 한 번에 잰다. 이력이 하나도 없으면 빈 값.
     *
     * <p><b>실행 시작에 한 번만 부른다.</b> 창마다 다시 재면 그 사이 커밋된 행 때문에
     * 경계가 달라져 결정론이 깨진다.
     */
    Optional<ReplayScanRange> scanRange(LocalDateTime asOf);

    /**
     * 구간의 이력을 {@code (issuance_id, created_at, id)} 오름차순으로 읽는다.
     *
     * @param fromIssuanceId 구간 시작. 포함
     * @param toIssuanceId   구간 끝. 포함
     * @param maxHistoryId   {@link ReplayScanRange#maxHistoryId()}. 실행 중 들어온 행을 막는 상한
     */
    List<IssuanceHistoryRecord> findRange(
            long fromIssuanceId, long toIssuanceId, LocalDateTime asOf, long maxHistoryId);
}
