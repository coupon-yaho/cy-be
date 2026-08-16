// 검증 규칙의 판정 질의 계약입니다. 규칙 하나가 메서드 하나입니다.
package com.kafkick.core.verification;

import java.time.LocalDateTime;
import java.util.List;

/**
 * <b>어긋난 것만 돌려줍니다.</b> 300만 행을 훑되 자바로 올라오는 것은 위반뿐입니다.
 * 정상셋에서는 0건이고 오염셋에서도 규칙당 최대 200건이라, 결과를 메모리에 담아도 됩니다.
 * 스캔이 큰 것이지 산출이 큰 것이 아닙니다.
 *
 * <p>그래서 {@code limit} 을 받습니다. 검증기 자체가 망가지면(예: 시각 비교가 밀리면)
 * 위반이 수백만 건으로 튀는데, 그것을 그대로 담으면 배치가 OOM 으로 죽고 원인이 묻힙니다.
 * 상한에 닿았다는 사실 자체가 <b>데이터가 아니라 검증기를 의심하라</b>는 신호입니다.
 */
public interface VerificationRuleRepository {

    /**
     * V3 리플레이 대조 — 접은 상태와 {@code issuances.status} 가 다른 발급건.
     *
     * <p>오염 유형 2(이력은 USED 인데 저장값은 ISSUED)가 이 규칙에 잡힙니다.
     *
     * <p><b>{@code asOf} 이후에 갱신된 발급건은 비교하지 않습니다.</b> 접힌 상태는 asOf 로 얼어 있는데
     * {@code issuances.status} 는 질의 순간의 현재값이라, 배치가 도는 동안 런타임이 건드린 발급건이
     * 전부 어긋난 것으로 잡힙니다. 정상셋에서 오탐이 나고 재실행 결과도 달라집니다.
     */
    List<VerificationFinding> findReplayMismatches(long runId, LocalDateTime asOf, int limit);

    /**
     * V5 사용 실적 정합 — 접은 상태와 활성 사용 건수가 어긋나는 발급건.
     *
     * <p>불변식은 <b>{@code USED} 면 활성 사용 1건, 아니면 0건</b>입니다.
     * 오염 유형 7(저장값은 ISSUED 인데 활성 사용 행이 남음)이 여기 잡히고,
     * 같은 식이 한 발급건의 이중 사용도 잡습니다.
     *
     * <p>{@code asof_state.active_usage_count} 는 Step 0 이 이미 채웠으므로
     * 이 규칙은 {@code issuance_usages} 를 다시 읽지 않습니다.
     */
    List<VerificationFinding> findUsageMismatches(long runId, int limit);

    /**
     * V1 재고 정합 — 접은 활성 건수와 {@code coupon_stocks.active_count} 가 다른 회차.
     *
     * <p>오염 유형 1(재고는 줄었는데 ISSUE 이력 없음)과 3(CANCEL_USE 이중 복원)이 여기 잡힙니다.
     *
     * <p><b>{@code coupons} 를 드라이빙으로 잡습니다.</b> 다른 둘은 각각 한쪽을 놓칩니다 —
     * {@code asof_state} 를 잡으면 <b>활성이 0인데 재고가 남은 회차</b>가,
     * {@code coupon_stocks} 를 잡으면 <b>재고 행이 없는데 발급이 쌓인 회차</b>가 빠집니다.
     * 둘 다 검출 대상이고, 뒤엣것은 초과 발급의 가장 위험한 형태입니다.
     * 회차가 CLEAN 147 · CORRUPT 291 뿐이라 전수 비용은 어느 쪽이든 같습니다.
     *
     * <p>활성은 {@code ISSUED} 와 {@code USED} 입니다 — 컬럼 주석이 "ISSUED + USED 합계" 라고
     * 못 박은 <b>현재 보유량</b>이지 누적 발급 수가 아닙니다.
     */
    List<VerificationFinding> findStockMismatches(long runId, LocalDateTime asOf, int limit);

    /**
     * V6 등급 자격 위반 — 발급 시점 등급이 회차의 허용 등급 마스크에 없는 발급건.
     *
     * <p><b>오염 유형이 없습니다.</b> 정상셋 0건으로만 검증되고, 위반을 손으로 심어 확인합니다.
     *
     * <p><b>{@code members} 를 조인하지 않습니다.</b> {@code members.membership_grade} 는 현재값이라
     * 회원이 강등되는 순간 정상 발급이 위반으로 잡힙니다. 발급 시점 스냅샷인
     * {@code issuances.issued_grade} 와 {@code coupons.eligible_grades_mask} 만 봅니다.
     *
     * <p><b>그렇다고 결정론은 아닙니다.</b> {@code issued_grade} 는 스냅샷이지만
     * {@code coupons.eligible_grades_mask} 는 살아 있는 행이고, {@code coupons} 에는
     * {@code updated_at} 컬럼이 없어 시각으로는 가드를 걸 수 없습니다. 지문 재료에도 그 축이 없어서
     * 마스크가 바뀌면 <b>지문은 같은데 검출만 달라집니다.</b>
     * 그래서 현재 행을 읽는 규칙들과 같은 구간(뒤쪽)에 둡니다.
     *
     * <p>그래도 <b>어떤 발급건을 볼지는 고정해야</b> 재실행 결과가 같습니다.
     * {@code updated_at <= asOf} 로 자릅니다 — V3 와 같은 기준이고 같은 가드가 이미 있습니다.
     */
    List<VerificationFinding> findGradeViolations(LocalDateTime asOf, int limit);

    /**
     * V2 1인 1매 위반 — 같은 회차에서 한 회원이 둘 이상 받은 것.
     *
     * <p><b>케이스가 둘인데 규칙은 하나입니다.</b> {@code target_key} 형식이 같아
     * 별도 규칙으로 나누면 같은 행이 두 규칙에 잡혀 집합 비교가 어긋납니다.
     *
     * <pre>
     * 케이스 1  GROUP BY coupon_id, member_id   같은 회원이 두 번        오염 유형 6
     * 케이스 2  GROUP BY coupon_id, code        같은 code 가 두 번       오염 유형 5
     * </pre>
     *
     * <p><b>케이스 2 는 {@code MIN(id)} 를 뺍니다.</b> code 가 겹치면 두 행이 나오는데
     * 먼저 발급된 쪽은 정상입니다. 안 빼면 <b>원본 회원까지 검출돼 오탐</b>이 되고,
     * 오염 100건이 200건으로 부풀어 집합 비교가 통째로 깨집니다.
     *
     * <p><b>결정론입니다.</b> {@code issuances} 만 읽고 그 테이블에는 {@code updated_at} 이 있어
     * {@code updated_at <= asOf} 로 자를 수 있습니다. 경계 가드는
     * {@link #hasIssuancesUpdatedAfter} 가 이미 갖고 있습니다.
     *
     * <p><b>CLEAN 스키마에서는 검출이 나올 수 없습니다.</b> {@code uk_coupon_member} 와
     * {@code issuances.code} UNIQUE 가 두 케이스를 물리적으로 막습니다. 그것이 정상이고,
     * 이 규칙은 <b>그 제약이 없는 CORRUPT 에서만</b> 의미가 있습니다 —
     * 테스트도 그래서 {@code CorruptRepositoryTest} 위에서 돕니다.
     */
    List<VerificationFinding> findDuplicateIssuances(LocalDateTime asOf, int limit);

    /**
     * asOf 이후에 갱신된 발급건이 있는가. <b>V3 의 선행 조건</b>이다.
     *
     * <p>V3 는 얼어 있는 접기 결과와 <b>현재</b> {@code issuances.status} 를 비교하므로,
     * asOf 이후 갱신된 행은 비교 대상에서 뺀다. 그런데 <b>빼기만 하면 0건이 두 가지 뜻을 갖는다</b> —
     * "제대로 훑고 아무것도 없었다" 와 "훑을 대상이 안 남았다" 가 구분되지 않는다.
     * 정상셋 0건이 합격 조건이라 후자가 녹색으로 통과한다.
     *
     * <p>그래서 실행 <b>시작과 끝에</b> 확인하고 하나라도 있으면 거부한다. 시작에만 보면
     * 그 뒤 몇 분 동안(리플레이 실측 57초 + 집계 + 규칙) 들어온 갱신을 아무도 다시 보지 않아,
     * 같은 asOf 두 실행이 다른 검출 집합을 내고도 둘 다 통과한다.
     *
     * <p>오염셋은 정적이어야 하는데
     * 주입기가 {@code updated_at} 을 주입 시각으로 찍으면 그 발급건들이 통째로 빠지고,
     * 기대 100건이 0건이 되어 "누락 100" 으로만 보인다 — 원인은 어디에도 안 남는다.
     */
    boolean hasIssuancesUpdatedAfter(LocalDateTime asOf);

    /**
     * {@code asOf} 이후에 갱신된 재고 행이 있는가.
     *
     * <p><b>V1 이 현재 {@code coupon_stocks.active_count} 를 읽기 때문에 필요합니다.</b>
     * 접은 활성 건수는 asOf 로 얼어 있는데 재고는 질의 순간의 현재값이라, 배치가 도는 동안
     * 발급이 한 건만 일어나도 그 회차가 어긋난 것으로 잡히고 재실행 결과가 달라집니다.
     *
     * <p>{@code issuances} 쪽과 같은 이유로 시작과 끝에서 두 번 봅니다. 그냥 빼면
     * <b>0건이 두 뜻을 갖습니다</b> — "제대로 훑고 없었다" 와 "훑을 대상이 안 남았다".
     */
    boolean hasStocksUpdatedAfter(LocalDateTime asOf);

    /**
     * 회차 정책의 지문. <b>V1·V6 이 읽는 {@code coupons} 축의 가드다.</b>
     *
     * <p>{@code coupons} 에는 {@code updated_at} 이 없어 시각으로 비교할 수 없다.
     * 대신 값을 접는다 — 회차는 CLEAN 147 · CORRUPT 291 행뿐이라 비용이 없다.
     *
     * <p><b>이 축이 없으면 오진이 난다.</b> 마스크가 바뀌면 검출은 달라지는데
     * {@code dataset_fingerprint} 재료에 그 축이 없어 지문은 같게 나온다.
     * 판정은 그 조합을 <i>"데이터는 그대로인데 검증기가 비결정적"</i> 으로 읽는데,
     * 실제로는 데이터가 바뀐 것이다 — 판정표에서 가장 찾기 어려운 칸이다.
     */
    String policyDigest();

    /**
     * 지금 보고 있는 스키마에 <b>CLEAN 전용 제약</b>이 살아 있는가.
     *
     * <p>{@code dataset} 파라미터는 {@code verification_runs} 에 적히는 <b>라벨일 뿐</b>이고,
     * 실제로 어느 스키마를 읽을지는 접속 URL 이 정합니다. 둘이 어긋나면
     * <b>CORRUPT DB 를 보면서 "CLEAN 에서 0건" 이라고 기록</b>할 수 있습니다 —
     * 이 프로젝트가 반복해서 막아 온 "0건이 두 뜻을 갖는다" 와 같은 종류입니다.
     *
     * <p>{@code uk_coupon_member} 로 판별합니다. CLEAN 전용 셋 중 이것이 가장 안정적입니다 —
     * {@code ck_stock_range} 는 {@code NOT ENFORCED} 로 살아 있을 수 있고
     * {@code code} 유일 인덱스는 이름이 저장소마다 다릅니다.
     */
    boolean hasCleanOnlyConstraints();
}
