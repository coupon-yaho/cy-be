// 기한이 지난 발급건을 만료로 넘기고 그만큼 재고를 되돌립니다.
package com.kafkick.core.expiration;

import java.time.LocalDateTime;

/**
 * <b>재고를 쓰는 유일한 잡이다.</b> 다른 배치는 원본을 읽기만 한다 — 그래서 동시성 테스트도,
 * 부하 중 정지도 이 잡에서만 필요하다.
 *
 * <p><b>락을 쓰지 않는다.</b> 조건부 {@code UPDATE} 의 매치 건수로 실제로 넘어간 수를 센다.
 * 락을 잡으면 서버가 죽었을 때 풀리지 않는 락이 남고, 그 하나가 발급 경로 전체를 멈춘다.
 *
 * <p><b>넘어간 집합을 {@code updated_at = committedAt} 로 다시 찾는다.</b> MySQL 에는
 * {@code UPDATE … RETURNING} 이 없어서, 방금 쓴 시각을 표식으로 삼는다.
 * 이력과 재고가 그 집합을 읽는다.
 *
 * <p><b>표식이 {@code asOf} 가 아니라 {@code committedAt} 인 것은 결정이다.</b> 쓰는 시각을
 * {@code asOf} 로 백데이트하면 잡이 도는 동안 들어온 이력보다 우리 이력이 앞서게 찍혀
 * 리플레이 정렬 {@code (issuance_id, created_at, id)} 이 뒤집힌다.
 *
 * <p><b>그리고 집합을 id 로 닫는다 — {@code (afterId, lastId]}.</b> 표식만으로 찾으면 질의가
 * 위쪽으로 열려 있어 테이블 끝까지 훑고, {@code INSERT … SELECT} 와 {@code UPDATE … JOIN} 은
 * 그 훑은 구간에 <b>공유 next-key 락</b>을 잡는다. supremum 까지 잠기므로
 * <b>{@code issuances} 로의 신규 INSERT — 발급 경로 자체가 막힌다.</b>
 * {@code mysql:latest} 에 재 봤다: 5,000행 중 1,000건을 넘길 때 상한이 없으면 락 5,020(S 4,016)
 * 이고 그 시각 발급 INSERT 가 오류 1205 로 죽는다. {@code id <= :lastId} 를 걸면 1,004 로 줄고
 * 발급이 통과한다. 지키는 것은 {@code ExpirationLockScopeTest} 다.
 *
 * <p><b>id 로 닫으면 표식의 전제도 함께 좁혀진다.</b> 예전에는 {@code EXPIRED} 를 쓰는 곳이
 * 이 잡뿐이어야 한다는 규칙에 기대야 했다 — 런타임이 같은 시각에 상태를 넘기면 남의 행이
 * 우리 집합에 섞였다. 이제 {@code (afterId, lastId]} 밖은 애초에 매치되지 않고,
 * 그 안쪽은 {@code expireBatch} 가 이미 X 락으로 쥐고 있어 남이 바꿀 수 없다.
 * {@code expires_at < :asOf} 를 함께 거는 것은 인덱스가 생겨 락 범위가 좁아지는 날을 위한
 * 두 번째 겹이다.
 *
 * <p><b>락 순서를 계약으로 못 박는다 — {@code issuances} → {@code issuance_histories}
 * → {@code coupon_stocks}.</b> 아래 여섯을 부르는 순서가 곧 이 순서다. 발급·사용·취소 경로도
 * <b>같은 순서로 잡아야 한다.</b>
 *
 * <p>반대로 잡으면 데드락이 난다. {@code mysql:latest} 컨테이너에 두 세션으로 재현했다 —
 * 한쪽이 {@code issuances} 를 잡은 채 {@code coupon_stocks} 를 기다리고 다른 쪽이 그 반대로
 * 기다리자 오류 <b>1213</b> 이 나면서 한쪽이 통째로 되돌아갔다. 재현에서 희생된 쪽은
 * 만료였다 — 되돌아가는 것이 만료면 <b>그 주기의 만료가 통째로 밀린다.</b>
 * 순서를 지키는 것은 {@code ExpireJobLockOrderTest} 다.
 */
public interface ExpirationRepository {

    /**
     * {@code id > afterId} 인 것 중 앞에서부터 {@code limit} 건을 만료로 넘긴다.
     *
     * <p><b>거르는 조건이 {@code UPDATE} 안에 있다.</b> 후보를 먼저 뽑았다가 그 사이 사용된 건을
     * 빼는 방식이면, 후보가 전부 사용된 청크에서 진도가 안 나가 같은 자리를 맴돈다.
     * 조건을 안에 두면 <b>0 은 곧 "남은 대상이 없다"</b> 가 되어 그것이 종료 신호다.
     *
     * @return 실제로 넘어간 건수. 그 사이 사용·취소된 건은 세지 않는다
     */
    int expireBatch(LocalDateTime asOf, LocalDateTime committedAt, long afterId, int limit);

    /**
     * 방금 넘어간 집합의 가장 큰 id. 다음 청크가 여기서 이어 간다.
     *
     * <p>넘어간 것이 없으면 {@code afterId} 를 그대로 돌려준다 — 0 으로 되돌리면 다음 청크가
     * 앞 구간을 다시 훑는다. 종료 판단은 {@link #expireBatch} 의 반환값으로 하므로 잡은 이
     * 경로를 밟지 않지만, 계약은 계약이라 {@code ExpirationJdbcAdapterTest} 가 도달시킨다.
     */
    long lastExpiredId(LocalDateTime asOf, LocalDateTime committedAt, long afterId);

    /**
     * 방금 넘어간 건마다 {@code EXPIRE} 이력을 한 줄 남긴다.
     *
     * <p>리플레이가 이 이력으로 상태를 재구성하므로, 이력이 없으면 검증이
     * <i>"이력 없는 발급건"</i> 으로 잡는다. 상태만 바꾸고 이력을 안 남기면 안 된다.
     *
     * <p>{@code (afterId, lastId]} 로 닫아 훑는 범위를 그 청크로 제한한다. 상한이 없으면
     * 이 문장이 테이블 끝까지 공유 락을 잡아 발급이 막힌다(클래스 주석의 실측).
     *
     * @return 쓴 이력 수. 넘어간 건수와 같아야 한다
     */
    int appendExpireHistories(LocalDateTime asOf, LocalDateTime committedAt,
            long afterId, long lastId);

    /**
     * 방금 넘어간 집합에 걸린 <b>회차 수</b>. {@link #stockRowCount} 가 센 행 수와 같아야 한다.
     *
     * <p>짝이 {@link #releaseStock} 이 아닌 것에 유의해라. 셋을 함께 봐야 두 실패가 갈린다 —
     * 이 값과 {@link #stockRowCount} 가 다르면 <b>재고 행이 없는 회차</b>,
     * {@link #stockRowCount} 와 {@link #releaseStock} 이 다르면 <b>뺄 재고가 모자란 회차</b>다.
     *
     * <p>같지 않으면 <b>재고 행이 없는 회차가 섞였다</b>는 뜻이다. 그 회차의 발급건은 만료로
     * 넘어갔는데 되돌릴 재고가 없다. {@code JOIN} 이 조용히 건너뛰므로 세지 않으면 아무도 모른다.
     */
    int expiredCouponCount(LocalDateTime asOf, LocalDateTime committedAt,
            long afterId, long lastId);

    /**
     * 넘어간 건수만큼 회차별 {@code active_count} 를 줄인다.
     *
     * <p><b>빼는 것이 맞다.</b> {@code active_count} 는 <i>ISSUED + USED 합계</i> 라
     * 만료는 그 합계에서 빠진다. 가용 재고가 그만큼 느는 것이 "재고 복원" 의 실체다.
     * 방향을 반대로 잡으면 완판 판정이 조용히 뒤집힌다.
     *
     * <p><b>뺄 수 있는 회차만 갱신한다({@code active_count >= 차감량}).</b> 음수를 막는 것이
     * {@code ck_stock_range} 뿐이면 그 제약을 떼어 낸 CORRUPT 스키마에서 음수가 그대로 커밋된다.
     * 불변식을 조건으로도 표현해 두면 스키마와 무관하게 막힌다.
     *
     * @return 갱신된 회차 수. {@link #stockRowCount} 와 다르면 뺄 재고가 모자란 회차가 있다
     */
    int releaseStock(LocalDateTime asOf, LocalDateTime committedAt, long afterId, long lastId);

    /**
     * 방금 넘어간 집합 중 <b>재고 행이 실제로 있는</b> 회차 수.
     *
     * <p>{@link #releaseStock} 이 갱신한 행 수와 짝을 이룬다. 셋을 함께 봐야 두 실패가 갈린다 —
     * {@link #expiredCouponCount} 와 다르면 <b>재고 행이 없는 회차</b>, 이 값과
     * {@link #releaseStock} 이 다르면 <b>뺄 재고가 모자란 회차</b>다. 하나로 뭉치면 원인이 섞인
     * 메시지가 나가고, 운영자가 엉뚱한 곳을 본다.
     */
    int stockRowCount(LocalDateTime asOf, LocalDateTime committedAt, long afterId, long lastId);
}
