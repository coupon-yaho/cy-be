// 회차의 시각 기반 상태 전이와, 밀린 전이를 세는 되읽기입니다.
package com.kafkick.core.coupon.port;

import java.time.LocalDateTime;
import java.util.List;

/**
 * <b>{@code port/} 에 둔다.</b> README 의 패키지 구조가 {@code core/coupon/{domain,service,port}}
 * 를 팀 규칙으로 정했고, 이 저장소는 <b>코드가 README 에 맞춘다</b>. 형제 도메인
 * ({@code core/expiration} · {@code core/verification})이 평면인 것은 그 규칙보다 앞선 코드다 —
 * 그것을 근거로 삼지 마라.
 *
 * <p><b>시각으로 닫히는 전이만 진다.</b> {@code SCHEDULED → OPEN}({@code open_at} 도달)과
 * {@code OPEN → CLOSED}({@code close_at} 도달) 둘이다.
 *
 * <p><b>재고 소진으로 닫는 것은 이 포트가 아니다.</b> 그건 {@code api} 의 발급 경로가 그
 * 자리에서 한다({@code docs/11} §"스케줄러는 전부 batch 에 있고"). 스케줄러를 기다리면
 * <b>재고가 0인데 {@code OPEN} 인 구간</b>이 생긴다. 그래서 선착순 쿠폰에서 <b>마감의 주된
 * 사유는 이 포트를 통과하지 않는다</b> — 여기 오는 것은 소진 안 된 회차뿐이다.
 *
 * <p><b>대상을 먼저 고르고 id 하나씩 조건부로 바꾼다.</b> 이 어댑터가 잠그는
 * {@code coupons} 는 <b>발급 경로가 같이 쓰는 테이블</b>이고, 그것이 이 설계의 전제다.
 * 실측(MySQL 8.0, 회차 150행 · {@code SCHEDULED} 10, 집합 {@code UPDATE} 를 열어 둔 상태):
 *
 * <table border="1">
 *   <caption>집합 UPDATE 의 락 범위 — 격리수준이 결과를 가른다</caption>
 *   <tr><th></th><th>{@code coupons} 락</th><th>발급 전 {@code FOR SHARE}</th>
 *       <th>재고소진 {@code CLOSED}</th></tr>
 *   <tr><td>{@code REPEATABLE READ}</td><td>X <b>151</b>(전부 + supremum)</td>
 *       <td><b>{@code ERROR 1205}</b></td><td><b>{@code ERROR 1205}</b></td></tr>
 *   <tr><td>{@code READ COMMITTED}</td><td>{@code X,REC_NOT_GAP} 10(매치된 행만)</td>
 *       <td>통과</td><td>통과</td></tr>
 * </table>
 *
 * <p>⚠️ <b>한때 이 표의 첫 줄만 재고 "집합 UPDATE 면 1분마다 발급이 막힌다" 고 적었다 —
 * 그건 기본 격리수준에서의 값이다.</b> 어댑터가 {@code READ COMMITTED} 로 돌면 집합
 * {@code UPDATE} 도 발급을 막지 않는다(semi-consistent read 가 안 맞는 행을 건너뛴다).
 * <b>발급을 살리는 결정은 격리수준이고 id 단건은 그것과 별개다.</b> 돌연변이로 확인했다 —
 * 집합 {@code UPDATE} 로 되돌려도 락 테스트가 전부 초록이었다.
 *
 * <p>그럼 id 단건이 사는 근거는 셋이다.
 * <ul>
 *   <li><b>격리수준이 되돌아가도 발자국이 한 행이다.</b> 위 표의 첫 줄이 그 대가다 —
 *       계약을 한 겹만 두면 그 겹이 벗겨지는 날 발급이 전면 실패한다.</li>
 *   <li><b>회차 단위 독립.</b> 집합 {@code UPDATE} 는 한 행이 실패하면 전부 롤백한다.
 *       회차 사이에 지켜야 할 불변식이 없으니 그럴 이유가 없다.</li>
 *   <li><b>회차 단위 관측.</b> 집합 {@code UPDATE} 는 <i>어느</i> 회차가 바뀌었는지 못 말한다 —
 *       실패 회차를 로그에 남기는 것도 못 한다.</li>
 * </ul>
 *
 * <p><b>전이마다 트랜잭션이 갈린다.</b> 회차 사이에 지켜야 할 불변식이 없고
 * (한 회차의 전이는 다른 회차와 무관하다) 전이는 멱등이다. 한 트랜잭션으로 묶으면 그 tick
 * 동안 대상 전부를 잠근 채로 있게 되는데, <b>락 보유 시간을 줄이는 것이 이 설계의 목적</b>이다.
 *
 * <p><b>{@code close_at} 은 갱신하지 않는다.</b> {@code docs/02} F5 가 정한 것이다 —
 * 갱신하면 <i>"언제 닫힐 예정이었나"</i> 가 소실되고, 실제 소진 시각은 마지막 {@code ISSUE}
 * 이력에서 계산한다.
 *
 * <p><b>⚠️ {@code open_at}·{@code close_at} 은 UTC 로 저장돼 있어야 한다.</b> 이 포트가 받는
 * {@code now} 는 {@code TimeProvider}({@code Clock.systemUTC})가 준 값이고, 비교는 원시 JDBC
 * 파라미터로 나가 <b>존 변환을 받지 않는다</b>({@code LocalDateTime} 에는 존이 없다).
 * 그래서 JVM 기본 존과는 무관하지만 — 테스트 JVM 은 {@code Asia/Seoul} 이고 운영 컨테이너는
 * {@code TZ=UTC} 다 — <b>저장된 값이 KST 면 전이가 그만큼 어긋난다.</b>
 *
 * <p>그 어긋남은 <b>알림에 안 잡힌다.</b> 대기 수를 세는 조회가 같은 비교를 쓰므로 게이지도
 * 0 이다. 여는 것이 9시간 늦고 아무 신호도 없다 — 시드나 회차 생성이 그 컬럼에 KST 를 쓰기
 * 시작하면 <b>여기가 먼저 조용히 틀린다.</b>
 *
 * <p><b>{@code coupon_stocks} 를 안 건드린다.</b> 재고를 쓰는 배치는 {@code expireJob}
 * 하나라는 계층 규칙이다.
 */
public interface CouponRoundRepository {

    /**
     * <b>열 수 있는 회차의 id.</b> {@code status='SCHEDULED'} · {@code open_at <= now} 이고
     * <b>재고 행이 있는 것</b>만 돌려준다.
     *
     * <p>⚠️ <b>빼는 것이 둘이다.</b> ① {@code coupon_stocks} 에 행이 없는 회차 — 열면 발급
     * 경로가 그 회차에서 죽는다. ② {@code close_at} 이 이미 지난 회차 — 열면 <b>마감 시각이
     * 지난 회차에서 발급이 나간다</b>. 둘 다 <b>조용히 깨는 것보다 보이게 멈추는 쪽</b>이고,
     * 각자 전용 게이지가 있다({@link PendingCounts#blockedByMissingStock} ·
     * {@link PendingCounts#missedWindow}). {@link PendingCounts#pendingOpen} 에는 둘 다 안 들어간다 —
     * 그래야 그 게이지의 뜻이 <i>"지금 열려 있어야 하는데 안 열렸다"</i> 하나로 남는다.
     */
    List<Long> roundsToOpen(LocalDateTime now);

    /** <b>닫을 회차의 id.</b> {@code status='OPEN'} · {@code close_at <= now}. */
    List<Long> roundsToClose(LocalDateTime now);

    /**
     * <b>대기 수 넷을 한 문장으로 센다.</b> 문장을 나누면 {@code READ COMMITTED} 에서
     * <b>문장마다 read view 가 새로 잡혀</b> 네 값이 서로 다른 시점의 DB 를 본다 —
     * 그 사이 재고 행이 생기거나 사라지면 회차 하나가 <b>어느 게이지에도 안 나오거나</b>
     * 둘에 <b>이중 계상</b>된다. 홀더를 하나로 묶는 것은 <i>발행</i>의 원자성이지
     * <i>읽기</i>의 일관성이 아니다.
     *
     * <p>회차는 백 단위라 한 문장으로 접는 비용이 없다. 왕복도 넷에서 하나로 준다.
     *
     * <p><b>넷은 서로 배타적이고 빠짐없다.</b> {@code SCHEDULED} 는
     * ① 창 안 + 재고 있음 → {@link PendingCounts#pendingOpen}
     * ② 창 안 + 재고 없음 → {@link PendingCounts#blockedByMissingStock}
     * ③ {@code close_at} 지남 → {@link PendingCounts#missedWindow}
     * ④ 아직 미래 → 아무 데도 안 셈(정상). {@code OPEN} 은 {@code close_at} 이 지났으면
     * {@link PendingCounts#pendingClose}, 아니면 안 셈. {@code CLOSED} 는 안 센다.
     * {@code close_at < open_at} 인 잘못된 행은 ③ 으로 드러난다.
     */
    PendingCounts countPending(LocalDateTime now);

    /**
     * <b>축을 갈라 둔 이유가 알림 채널이다.</b> {@code pendingOpen} 은 <b>서버</b>를 봐야 하는
     * 상태이고, {@code blockedByMissingStock} 과 {@code missedWindow} 는 <b>데이터</b>다 —
     * 서버를 고쳐도 안 사라진다. 이 저장소는 <i>"데이터가 틀렸다는 판정과 배치가 일을 안
     * 한다는 판정을 같은 알람으로 묶지 않는다"</i> 를 규칙으로 굳혀 놨다.
     *
     * @param pendingOpen 지금 열려 있어야 하는데 아직 {@code SCHEDULED} 인 회차 수
     * @param pendingClose {@code close_at} 이 지났는데 아직 {@code OPEN} 인 회차 수
     * @param missedWindow {@code SCHEDULED} 인데 {@code close_at} 도 이미 지난 회차 수
     * @param blockedByMissingStock 열려야 하지만 재고 행이 없어 못 여는 회차 수
     */
    record PendingCounts(int pendingOpen, int pendingClose, int missedWindow,
            int blockedByMissingStock) {
    }

    /**
     * {@code SCHEDULED → OPEN}. 바뀌었으면 {@code true}.
     *
     * <p>{@code false} 는 오류가 아니다 — 그 사이 누가 상태를 바꿨다는 뜻이고, 이 전이가
     * 조건부인 이유가 그것이다.
     *
     * <p><b>가드 하나만 봐도 안전해야 한다.</b> 여는 가드는 상태와 <b>창</b> 둘을 보고
     * ({@code close_at > now}) 닫는 가드도 창을 본다({@code close_at <= now}) — 둘 다
     * {@code now} 를 받는 이유가 그것이다. 조회가 이미 걸러 주지만, <b>다음 사람이 조회를
     * 고칠 때 이 문장이 스스로를 지켜야</b> 한다. 한쪽에만 걸면 그 계약이 반쪽이 된다 —
     * {@link #roundsToClose} 에서 시각 조건이 떨어지는 날 <b>진행 중인 회차가 전부 닫힌다</b>.
     *
     * <p>그 {@code now} 는 {@link #roundsToOpen}·{@link #roundsToClose} 가 쓴 것과 <b>같은
     * 시각</b>이어야 한다. 여기서 다시 읽으면 tick 안에서 기준이 갈려, 조회는 하라고 하고
     * 갱신은 거부하는 조합이 난다.
     */
    boolean open(long couponId, LocalDateTime now);

    /**
     * {@code OPEN → CLOSED}. 바뀌었으면 {@code true}.
     *
     * <p><b>{@code false} 가 정상 경로에 있다.</b> 재고 소진으로 발급 경로가 먼저 닫으면
     * 여기는 0행이다. 그것을 실패로 세면 <b>가장 흔한 마감이 매번 오류로 보고된다.</b>
     */
    boolean close(long couponId, LocalDateTime now);


}
