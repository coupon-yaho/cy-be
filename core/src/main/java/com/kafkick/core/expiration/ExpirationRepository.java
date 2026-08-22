// 기한이 지난 발급건을 만료로 넘기고 그만큼 재고를 되돌립니다.
package com.kafkick.core.expiration;

import java.time.LocalDateTime;
import java.util.List;

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
 * 위쪽으로 열려 있어 테이블 끝까지 훑는다. 지금(READ COMMITTED)은 그것이 <b>스캔 비용</b>이고,
 * 격리가 RR 로 되돌아가면 {@code INSERT … SELECT} 와 {@code UPDATE … JOIN} 이 supremum 까지
 * 잠가 <b>발급 INSERT 를 죽인다</b> — 상한은 그날의 마지막 겹이기도 하다.
 *
 * <p><b>발급 봉쇄를 푼 것은 상한이 아니라 격리 수준이다.</b> 상한은 첫 문장에 걸 수도 없다.
 * 수치와 그 경위는 {@code docs/12-expire-lock-measurement.md} 에 있다.
 *
 * <p><b>id 로 닫으면 표식의 전제도 함께 좁혀진다.</b> 예전에는 {@code EXPIRED} 를 쓰는 곳이
 * 이 잡뿐이어야 한다는 규칙에 기대야 했다 — 런타임이 같은 시각에 상태를 넘기면 남의 행이
 * 우리 집합에 섞였다. 이제 {@code (afterId, lastId]} 밖은 애초에 매치되지 않는다.
 *
 * <p><b>다만 id 구간은 범위를 좁힐 뿐 배타를 주지 않는다.</b> {@code expireBatch} 가 X 락으로
 * 쥐는 것은 <b>매치된 {@code ISSUED} 행뿐</b>이고, 같은 구간의 {@code USED}·{@code CANCELLED} 는
 * 안 잡는다(인덱스 범위 스캔이라 읽지도 않는다). 표식이 유일하다는 것을 실제로 보장하는 것은
 * <b>{@code CouponStateMachine} 의 전이표에 {@code EXPIRED} 로 가는 길이
 * {@code EXPIRE: ISSUED → EXPIRED} 하나뿐</b>이라는 사실이다.
 * 그 전이표에 두 번째 길이 생기는 날 이 표식 방식을 다시 봐야 한다.
 *
 * <p><b>락 순서를 계약으로 못 박는다 — {@code issuances} → {@code issuance_histories}
 * → {@code coupon_stocks}.</b> 계약을 지는 것은 <b>쓰는 셋</b>이다 —
 * {@link #expireBatch} → {@link #appendExpireHistories} → {@link #releaseStock}.
 * 발급·사용·취소 경로도 <b>같은 순서로 잡아야 한다.</b>
 *
 * <p><b>나머지는 락을 안 잡는 읽기라 이 순서 밖이다</b> — {@link #blockedCoupons} ·
 * {@link #countPending} · {@link #lastExpiredId} · {@link #expiredCouponCount} ·
 * {@link #stockRowCount}. 선언 순서를 계약으로 읽지 마라. 새 <b>쓰기</b> 메서드를 더할 때
 * 자리를 정하는 것은 선언 위치가 아니라 이 문단이고, 실제로 지키는 것은
 * {@code ExpireJobLockOrderTest} 다.
 *
 * <p>반대로 잡으면 데드락이 난다. {@code mysql:latest} 컨테이너에 두 세션으로 재현했다 —
 * 한쪽이 {@code issuances} 를 잡은 채 {@code coupon_stocks} 를 기다리고 다른 쪽이 그 반대로
 * 기다리자 오류 <b>1213</b> 이 나면서 한쪽이 통째로 되돌아갔다. 재현에서 희생된 쪽은
 * 만료였다 — 되돌아가는 것이 만료면 <b>그 주기의 만료가 통째로 밀린다.</b>
 * 순서를 지키는 것은 {@code ExpireJobLockOrderTest} 다.
 */
public interface ExpirationRepository {

    /**
     * <b>지금 만료시킬 수 없는 회차를 미리 가른다.</b> 재고 행이 없거나, 남은 만료 대기 건수를
     * 다 빼면 {@code active_count} 가 음수가 되는 회차다.
     *
     * <p><b>왜 미리 가르나.</b> 예전에는 넘긴 뒤에 가드가 그것을 발견하고 청크를 통째로
     * 되돌렸다 — 오염 회차 하나가 같은 청크의 남의 회차까지 되돌리고, 진도가 실행 사이로
     * 안 넘어가니 다음 주기도 같은 자리에서 죽어 <b>그 뒤 id 의 만료가 영구히 밀렸다.</b>
     * 설계는 <i>"데이터가 틀렸다는 판정이 나와도 배치는 정상 종료"</i> 로 정했는데 그 반대였다.
     *
     * <p><b>청크와 무관한 성질로 정의한다.</b> "이 청크에서 모자란다" 로 정의하면 제외한 만큼
     * {@code LIMIT} 자리가 비어 <b>다른 행이 창 안으로 들어오고</b>, 그 회차가 또 막혀 있으면
     * 재고 없이 만료된 상태가 커밋된다. 남은 대기 <b>전체</b>와 견주면 제외 대상이 창 구성과
     * 무관해져서, 밀려 들어오는 것은 언제나 성한 회차뿐이다.
     *
     * <p>그래서 조금 보수적이다 — 대기 5건에 재고 3인 회차는 3건도 안 넘기고 통째로 빠진다.
     * 이미 어긋난 회차이므로 부분 처리보다 손대지 않는 편이 낫다.
     *
     * <p><b>{@code committedAt} 을 안 받는 것이 결정이다.</b> {@code expireBatch} 의
     * 캡처 창({@code updated_at <= committedAt})을 여기도 걸면 이 값이 <i>"그 시각 기준의
     * 대기"</i> 가 되는데, {@code committedAt} 은 청크마다 새로 잡혀 <b>뒤 청크의 창이 더
     * 넓다.</b> 그 사이에 갱신된 행이 여기 안 세졌는데 만료는 되어, 회차별 차감 합계가
     * 이 값을 넘고 {@code STOCK_UNDERFLOW} 로 죽는다. 창을 빼면 이 값은 그 실행이 넘길 수
     * 있는 모든 행의 <b>상계</b>가 되어, 어떤 {@code committedAt} 수열에서도 부등식이 선다.
     *
     * <p><b>실행당 한 번만 부른다.</b> 청크마다 부르면 남은 후보 전체를 매번 훑는다.
     * 결과는 그 {@code JobExecution} 의 {@code ExecutionContext} 에 실린다 —
     * <b>재시작은 다시 계산한다.</b> 첫 실행과 재시작 사이에 어긋난 회차를 낡은 목록이
     * 못 보면 재시작이 같은 자리에서 영원히 죽는다.
     */
    List<Long> blockedCoupons(LocalDateTime asOf);

    /**
     * 기한이 지났는데 아직 {@code ISSUED} 인 발급건 수. <b>실행이 끝난 뒤 한 번 센다.</b>
     *
     * <p>스크레이프 때 세면 300만 행에 {@code COUNT(*)} 를 15초마다 때리는 꼴이다.
     * 잡이 끝나는 시점이면 이미 같은 창을 훑고 있었다.
     *
     * <p>{@code blockedCouponIds} 를 받아 <b>둘로 갈라 센다</b> — 막힌 회차의 몫은 설계상
     * 계속 남으므로, 합쳐서 알림을 걸면 사람이 재고를 고칠 때까지 24시간 울리고 그 알림은
     * 곧 무시된다. 갈라야 <i>"배치가 일을 안 한다"</i> 와 <i>"데이터가 어긋나 있다"</i> 가
     * 서로 다른 알림이 된다.
     */
    PendingExpiration countPending(LocalDateTime asOf, List<Long> blockedCouponIds);

    /**
     * {@code id > afterId} 인 것 중 앞에서부터 {@code limit} 건을 만료로 넘긴다.
     *
     * <p><b>거르는 조건이 {@code UPDATE} 안에 있다.</b> 후보를 먼저 뽑았다가 그 사이 사용된 건을
     * 빼는 방식이면, 후보가 전부 사용된 청크에서 진도가 안 나가 같은 자리를 맴돈다.
     * 조건을 안에 두면 <b>0 은 곧 "남은 대상이 없다"</b> 가 되어 그것이 종료 신호다.
     *
     * @param blockedCoupons 이 실행에서 손대지 않을 회차. {@link #blockedCoupons} 가 준 것을
     *                       그대로 넘긴다. <b>빈 목록이 정상이다</b> — 오염이 없는 날이
     *                       대부분이다. {@code NOT IN ()} 이 문법 오류라는 사정은 어댑터가
     *                       센티널로 처리하므로 부르는 쪽은 신경 쓰지 않는다
     * @return 실제로 넘어간 건수. 그 사이 사용·취소된 건은 세지 않는다
     */
    int expireBatch(LocalDateTime asOf, LocalDateTime committedAt, long afterId, int limit,
            List<Long> blockedCoupons);

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
     * 방금 넘어간 집합 중 <b>재고 행이 실제로 있는</b> 회차 수.
     *
     * <p>{@link #releaseStock} 이 갱신한 행 수와 짝을 이룬다. 셋을 함께 봐야 두 실패가 갈린다 —
     * {@link #expiredCouponCount} 와 다르면 <b>재고 행이 없는 회차</b>, 이 값과
     * {@link #releaseStock} 이 다르면 <b>뺄 재고가 모자란 회차</b>다. 하나로 뭉치면 원인이 섞인
     * 메시지가 나가고, 운영자가 엉뚱한 곳을 본다.
     */
    int stockRowCount(LocalDateTime asOf, LocalDateTime committedAt, long afterId, long lastId);

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
}
