// 기한이 지난 발급건을 만료로 넘기고 그만큼 재고를 되돌립니다.
package com.kafkick.core.expiration;

import java.time.LocalDateTime;
import java.util.List;

/**
 * <b>재고를 쓰는 유일한 잡이다.</b> 다른 배치는 원본을 읽기만 한다 — 그래서 동시성 테스트도,
 * 부하 중 정지도 이 잡에서만 필요하다.
 *
 * <p><b>재고 행 하나를 먼저 잠근다({@link #lockStock}).</b> 나머지는 조건부 {@code UPDATE} 의
 * 매치 건수로 실제로 넘어간 수를 센다 — 발급건 쪽에는 락을 안 잡는다.
 *
 * <p><b>한때는 아무것도 안 잠갔다.</b> 이유는 <i>"서버가 죽으면 풀리지 않는 락이 남고 그 하나가
 * 발급 경로 전체를 멈춘다"</i> 였다. 그 걱정은 <b>절반만 맞다.</b>
 *
 * <ul>
 *   <li><b>프로세스가 죽는 경우 — 서버가 풀어 준다. 다만 즉시가 아니다.</b> 락을 쥔 클라이언트를
 *       컨테이너째 SIGKILL 하고 재 봤다(MySQL 8.4): <b>4초 시점에는 락이 남아 있었고 14초
 *       시점에는 풀려 있었다.</b> 그 사이가 TCP 종료를 서버가 알아채는 시간이다.</li>
 *   <li><b>호스트·네트워크가 멎는 경우 — 안 쟀다.</b> 그때는 서버 쪽 세션이 읽기에서 계속
 *       블록되므로 {@code wait_timeout}(기본 8시간)이나 TCP keepalive 까지 남을 수 있다.
 *       하필 그 행이 <b>선착순 판정의 직렬화 지점</b>이라 값이 크다.
 *       {@code docs/12-expire-lock-measurement.md} §8 에 안 잰 것으로 올려 뒀다.</li>
 * </ul>
 *
 * <p>그럼에도 잠그는 쪽으로 온 것은 <b>안 잠근 대가가 더 컸기 때문</b>이다: 아래 데드락이다.
 * 걱정의 크기를 비교한 것이지 걱정이 사라진 것이 아니다.
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
 * <h2>락 순서 — {@code issuances} → {@code issuance_histories} → {@code coupon_stocks}</h2>
 *
 * <p>계약을 지는 것은 <b>쓰는 넷</b>이다 — {@link #expireBatch}
 * → {@link #appendExpireHistories} → {@link #lockStock} → {@link #releaseStock}.
 *
 * <p><b>이 순서는 우리가 고른 것이 아니라 시스템에 이미 있던 것이다.</b>
 * <b>재고를 건드리는 경로는 전부 {@code coupon_stocks} 를 마지막에 건드린다.</b>
 *
 * <table border="1">
 *   <caption>사용자 경로가 재고 행을 언제 잠그나</caption>
 *   <tr><td>{@code CouponIssueService}</td><td>마지막 — 조건부 {@code occupyOne}</td></tr>
 *   <tr><td>{@code CouponCancelService}</td><td>마지막 — 조건부 {@code release}</td></tr>
 *   <tr><td>{@code CouponCancelUseService}</td>
 *       <td><b>{@code EXPIRED} 로 갈 때만.</b> 보통 경로({@code USED → ISSUED})는 재고를
 *           아예 안 건드려 이 순서 밖이다 — 그리고 <b>안 기다리므로 순환도 안 만든다</b></td></tr>
 * </table>
 *
 * <p><b>이 방향이 2026-08-30 에 뒤집혔다.</b> 그전에는 발급이 재고를 먼저 잠갔고, 그때
 * 근거는 <i>"재고 판정이 곧 선착순 판정이라 그 행이 직렬화 지점"</i> 이었다. CY-750 이
 * 선착순 판정을 <b>조건부 원자 UPDATE</b> 로 옮기면서 앞에서 따로 잠글 이유가 사라졌고,
 * 그러면 바꿀 수 있는 쪽이 만료가 된다({@code docs/12} §11.1).
 *
 * <p><b>어느 방향이든 하나만 반대면 데드락이 난다. 두 번 다 재현했다</b> —
 * MySQL 8.4 · READ COMMITTED · 두 세션. 한쪽이 {@code issuances} 를 잡은 채 재고를 기다리고
 * 다른 쪽이 그 반대로 기다리면 오류 <b>1213</b> 이다. <b>6/6, 뜨는 순서와 무관.</b>
 *
 * <p><b>희생되는 쪽은 언제나 취소였다.</b> 취소는 그 시점까지 {@code FOR UPDATE} 읽기만 해서
 * undo 가 비어 있고, InnoDB 는 <b>한 일이 적은 쪽</b>을 죽인다. 그래서 운영에서 보이는 모습이
 * 이렇게 된다 — <b>사용자 취소가 간헐적으로 실패하는데 배치 로그는 깨끗하다.</b>
 * 이 잡의 메트릭 어디에도 안 잡힌다.
 *
 * <p><b>발급은 이 데드락에 안 걸린다(0/3).</b> 새 행을 {@code INSERT} 하므로 만료가 쥔 기존
 * 행을 기다릴 일이 없고, 재고 행에서 <b>직렬화만</b> 된다. 걸리는 것은 기존 발급건을 고치면서
 * 재고를 건드리는 둘 — <b>취소와 사용취소</b>다.
 *
 * <p><b>나머지는 락을 안 잡는 읽기라 이 순서 밖이다</b> — {@link #blockedCoupons} ·
 * {@link #countPending} · {@link #nextCandidates}. 선언 순서를 계약으로 읽지 마라.
 * 새 <b>쓰기</b> 메서드를 더할 때 자리를 정하는 것은 선언 위치가 아니라 이 문단이고,
 * 실제로 지키는 것은 {@code ExpireJobLockOrderTest} 다.
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
     * <p><b>{@code committedAt} 이 창을 닫는다.</b> 그 실행이 마지막으로 쓴 시각 이후에
     * 바뀐 행은 <b>그 실행이 처리했어야 하는 몫이 아니다</b> — 실행이 끝난 뒤
     * {@code CANCEL_USE}({@code USED → ISSUED})로 돌아온 행이 그렇다. 창이 없으면 그것이
     * {@code unexplained} 로 들어가 {@code ExpireLeavesWorkBehind}(critical · channel server)
     * 가 뜬다: <b>배치는 안 틀렸는데 서버를 보라고 나가고, 만료가 일 1회라 최대 하루 간다.</b>
     *
     * <p>⚠️ <b>{@code null} 이면 창을 안 건다.</b> 이 축은 CY-768 이 새로 만든 것이라
     * <b>배포 직후 마지막 성공 실행에는 반드시 없다.</b> 그때 판정을 포기하면
     * {@code ExpireMetricsUnknown} 이 하루를 우는데, 그것은 고치려던 오탐을 다른 오탐으로
     * 바꾸는 것이다. 없으면 지금까지의 동작 그대로 세고, 새 실행이 한 번 돌면 창이 걸린다.
     *
     * <p>⚠️ <b>이 창은 쓰기 시각이 정직해야 뜻이 있다.</b> 취소가 <b>요청 시각</b>으로
     * 백데이트되면 창 안에 그대로 들어와 안 걸러진다 — CY-769 가
     * {@code GREATEST(..., CURRENT_TIMESTAMP(6))} 로 그것을 닫았다.
     *
     * <p>{@code blockedCouponIds} 를 받아 <b>둘로 갈라 센다</b> — 막힌 회차의 몫은 설계상
     * 계속 남으므로, 합쳐서 알림을 걸면 사람이 재고를 고칠 때까지 24시간 울리고 그 알림은
     * 곧 무시된다. 갈라야 <i>"배치가 일을 안 한다"</i> 와 <i>"데이터가 어긋나 있다"</i> 가
     * 서로 다른 알림이 된다.
     */
    PendingExpiration countPending(LocalDateTime asOf, Long maxHistoryId,
            List<Long> blockedCouponIds);

    /**
     * <b>지금까지 매겨진 이력 id 의 최댓값.</b> 만료 실행이 끝나며 한 번 찍어 Step 문맥에
     * 싣고, 되읽기가 그것을 창으로 쓴다.
     *
     * <p><b>시각이 아니라 id 인 이유.</b> {@code issuance_histories.created_at} 은
     * <b>멱등 선점 시각</b>이라 백데이트된다 — 창 이전에 선점되고 창 이후에 커밋된 취소가
     * 창을 그대로 통과한다. id 는 {@code INSERT} 시점에 매겨지고 뒤로 안 간다.
     * 검증이 {@code hasHistoriesAddedAbove(frozenMaxHistoryId, …)} 로 같은 축을 이미 쓴다.
     */
    long latestHistoryId();

    /**
     * 이 청크가 <b>건드릴 후보</b>를 id 오름차순으로 {@code limit} 건까지 읽는다. <b>락을 안 잡는다.</b>
     *
     * <h2>왜 미리 읽나</h2>
     *
     * <p>재고 행을 <b>먼저</b> 잠가야 하는데({@link #lockStock}), 어느 회차의 행을 잠글지는
     * {@code UPDATE} 를 돌려 봐야 알 수 있었다. 그 순서를 뒤집으려면 어느 회차인지가
     * <b>쓰기 전에</b> 정해져 있어야 한다. 그래서 후보를 먼저 본다.
     *
     * <p><b>여기서 읽은 건수를 만료 건수로 쓰면 안 된다.</b> 락을 안 잡으므로 이 사이에
     * 사용·취소가 들어올 수 있다. 실제로 넘어간 수는 {@link #expireBatch} 의 매치 건수뿐이다.
     *
     * <p><b>대신 진도는 여기서 나온다.</b> 예전에는 {@code UPDATE} 가 0 을 돌려주는 것이
     * 종료 신호였는데, 그러면 <b>후보가 전부 사용된 청크에서 진도가 안 나가</b> 같은 자리를
     * 맴돈다. 이제 종료 신호는 <b>후보 0건</b>이고, 넘어간 것이 없어도 {@code afterId} 는
     * {@link ExpireChunk#lastId} 까지 밀린다.
     *
     * <p><b>{@code ORDER BY id} 가 계약이다.</b> {@link ExpireChunk#from} 이 그 위에서
     * 연속부를 자른다 — 순서가 어긋나면 자른 구간 밖에 같은 회차의 대상이 남는데
     * {@code afterId} 는 그 위로 밀려 <b>그 건들이 영영 안 넘어간다.</b>
     *
     * @param blockedCoupons 이 실행에서 손대지 않을 회차. {@link #blockedCoupons} 가 준 것을
     *                       그대로 넘긴다. <b>빈 목록이 정상이다</b>
     */
    List<ExpireCandidate> nextCandidates(LocalDateTime asOf, long afterId, int limit,
            List<Long> blockedCoupons);

    /**
     * 회차의 재고 행을 {@code SELECT … FOR UPDATE} 로 잠근다. <b>청크의 마지막 쓰기 락이다</b>
     * — 발급·취소·사용취소가 재고를 마지막에 건드리므로 만료도 그 자리에서 잡는다(CY-750).
     * 잠그는 이유는 순서가 아니라 <b>진단</b>이다: {@link #releaseStock} 이 0 을 돌려줬을 때
     * <i>"행이 없다"</i> 와 <i>"재고가 모자란다"</i> 를 가른다.
     *
     * <p><b>이 문장이 {@link #releaseStock} 바로 앞이어야 한다.</b> 앞으로 옮기면 재고 행을
     * 청크 내내 쥐게 되고, 재고를 마지막에 건드리는 사용자 경로와 순서가 역전돼 순환이
     * 생긴다 — 클래스 주석의 1213 재현이 그 이유다.
     *
     * <p><b>잠그기만 하고 아무것도 안 읽는다.</b> 뺄 수 있는지는 {@link #releaseStock} 의
     * {@code active_count >= n} 조건이 판단한다. 여기서 값을 읽어 자바에서 비교하면
     * <b>읽은 값과 쓰는 값 사이가 벌어진다</b> — 지금은 우리가 락을 쥐고 있어 안 벌어지지만,
     * 그 안전이 <i>"락을 쥐고 있다"</i> 는 사실 하나에만 걸리게 된다.
     *
     * @return 재고 행이 있으면 {@code true}. <b>{@code false} 는 사고다</b> —
     *         {@link #blockedCoupons} 가 재고 행 없는 회차를 이미 걸렀어야 한다
     */
    boolean lockStock(long couponId);

    /**
     * {@code (afterId, lastId]} 구간에서 <b>그 회차의</b> 만료 대상을 넘긴다.
     *
     * <p><b>거르는 조건이 {@code UPDATE} 안에 있다.</b> 후보를 미리 읽긴 하지만 그것은
     * <b>범위를 정하는 용도</b>일 뿐이고, 넘길지 말지는 여기서 다시 판단한다. 그 사이에
     * 사용된 건은 여기서 매치가 안 되고, 반환값이 후보 수보다 작아진다.
     *
     * <p><b>0 이 종료 신호가 아니다.</b> 그 자리는 {@link #nextCandidates} 가 진다.
     * 여기서 0 은 <i>"그 구간이 전부 사용·취소됐다"</i> 는 뜻이고, 그때도 진도는 나간다.
     *
     * @param couponId {@link ExpireChunk#couponId}. 이 회차의 만료 대상만 매치한다 —
     *                 재고 락은 이 문장 <b>뒤</b>에 잡는다
     * @return 실제로 넘어간 건수
     */
    int expireBatch(LocalDateTime asOf, LocalDateTime committedAt, long afterId, long lastId,
            long couponId);

    /**
     * 방금 넘어간 건마다 {@code EXPIRE} 이력을 한 줄 남긴다.
     *
     * <p>리플레이가 이 이력으로 상태를 재구성하므로, 이력이 없으면 검증이
     * <i>"이력 없는 발급건"</i> 으로 잡는다. 상태만 바꾸고 이력을 안 남기면 안 된다.
     *
     * <p>{@code (afterId, lastId]} 로 닫아 훑는 범위를 그 청크로 제한한다. 상한이 없으면
     * 이 문장이 테이블 끝까지 공유 락을 잡아 발급이 막힌다.
     *
     * <p><b>회차도 함께 좁힌다.</b> 그 구간은 회차가 섞일 수 있고 — 연속부 자르기는 후보
     * 목록 안에서만 연속이다 — 청크가 회차 하나로 정해진 뒤로는 그 조건을 거는 비용이 0 이다.
     * 그러면 이 문장이 표식({@code updated_at = committedAt})의 유일성에 기대지 않는다.
     *
     * @return 쓴 이력 수. 넘어간 건수와 같아야 한다
     */
    int appendExpireHistories(LocalDateTime asOf, LocalDateTime committedAt,
            long afterId, long lastId, long couponId);

    /**
     * 넘어간 건수만큼 그 회차의 {@code active_count} 를 줄인다.
     *
     * <p><b>빼는 것이 맞다.</b> {@code active_count} 는 <i>ISSUED + USED 합계</i> 라
     * 만료는 그 합계에서 빠진다. 가용 재고가 그만큼 느는 것이 "재고 복원" 의 실체다.
     * 방향을 반대로 잡으면 완판 판정이 조용히 뒤집힌다.
     *
     * <p><b>뺄 수 있을 때만 갱신한다({@code active_count >= expired}).</b> 음수를 막는 것이
     * {@code ck_stock_range} 뿐이면 그 제약을 떼어 낸 CORRUPT 스키마에서 음수가 그대로 커밋된다.
     * 불변식을 조건으로도 표현해 두면 스키마와 무관하게 막힌다.
     *
     * <p><b>한때는 {@code JOIN … GROUP BY} 로 회차별 합계를 접었다.</b> 청크가 여러 회차에
     * 걸쳤기 때문이다. 이제 청크가 회차 하나라 접을 것이 없고, 넘어간 수도 이미 알고 있다.
     *
     * @return 갱신된 행 수. <b>1 이어야 한다</b> — 0 이면 뺄 재고가 모자란 회차다.
     *         재고 행이 없는 경우는 여기 오지 않는다({@link #lockStock} 이 먼저 잡는다)
     */
    int releaseStock(long couponId, int expired, LocalDateTime committedAt);
}
