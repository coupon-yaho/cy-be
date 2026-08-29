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
     * 얼린 이력 상한 <b>위로</b>(= 더 큰 id 로) {@code asOf} 이하 이력이 끼어들었는가.
     * 이름을 SQL 방향과 맞춘다 — 이 저장소는 어휘가 뒤집혀 있어 이름 오해가 가장 흔한 사고다.
     *
     * <p>리플레이는 {@code id <= maxHistoryId} 로 자르는데, {@code dataset_fingerprint} 는
     * {@code MAX(id) WHERE created_at <= asOf} 를 <b>다시 잰다.</b> 실행 중에 백데이트 이력이
     * 들어오면 리플레이는 못 읽고 지문은 읽어, <b>같은 지문에 다른 검출</b>이 나온다 —
     * 판정표가 "검증기 버그" 로 잘못 읽는 바로 그 칸이다.
     *
     * <p>발급건 시각 가드가 간접 방어라는 반론이 가능하지만, <b>이력만 넣고 {@code issuances} 를
     * 안 건드리는 것이 오염 주입의 기본 모양</b>이다 — 유형 3 이 정확히 그렇다.
     *
     * <p>PK 범위 조회라 비용이 없다.
     */
    boolean hasHistoriesAddedAbove(long frozenMaxHistoryId, LocalDateTime asOf);

    /**
     * 얼린 사용 상한 <b>위로</b> {@code asOf} 이하 사용 이력이 끼어들었는가.
     * 이름과 SQL 방향은 형제 {@link #hasHistoriesAddedAbove} 와 맞춘다.
     *
     * <p><b>창의 컬럼은 형제와 다르다</b> — 형제는 {@code created_at}(리플레이가 정렬·필터에
     * 쓰는 그 컬럼), 이쪽은 {@code used_at} 이다. V5 가 {@code used_at <= asOf} 로 세기
     * 때문이다. 이 가드가 답해야 할 질문이 <i>"V5 가 그 행을 셀 것인가"</i> 라서,
     * V5 와 같은 술어를 써야 뜻이 맞는다.
     *
     * <p><b>V5 가 읽는 다섯째 축인데 얼림 가드에도 지문에도 없었다.</b>
     * {@code assertFrozenStep} 은 네 축(발급건·재고·회차 정책·이력)만 보고,
     * {@code dataset_fingerprint} 재료 다섯에도 {@code issuance_usages} 가 없다.
     *
     * <p>그 조합이 만드는 칸이 나쁘다 — <b>usages 행만 넣고 {@code issuances} 를 안 건드리면
     * V5 의 답은 달라지는데 지문은 그대로다.</b> 판정표는 그것을
     * <i>"지문 같음 + checksum 다름 = 검증기 버그"</i> 로 읽는다. 형제 javadoc 이 이력 축에
     * 대해 적은 <i>"이력만 넣고 issuances 를 안 건드리는 것이 오염 주입의 기본 모양"</i> 이
     * 여기에도 그대로 적용된다.
     *
     * <p><b>지문은 안 고친다.</b> 그것은 계약({@code contract.json})이 정한 다섯 항이라
     * 여기서 늘리면 시드와 갈린다. 그래서 <b>가드로 막는다.</b>
     *
     * <p><b>이 가드가 홀로 덮는 범위는 좁다.</b> 애플리케이션 경로는
     * {@code CouponUseService}·{@code CouponCancelUseService} 가 사용 행과 이력 행을
     * <b>한 트랜잭션·같은 시각</b>으로 함께 쓰므로, 그 경로에서 이 가드가 발화하는 상황은
     * 형제 이력 축 가드가 이미 발화하는 상황이다. 홀로 덮는 것은
     * <b>DB 에 직접 친 INSERT</b>(수동 오염 주입·시드 스크립트)다. 그래도 지킬 값이 있는
     * 것은 오염 주입이 정확히 그 모양이기 때문이다.
     *
     * <p><b>기존 행의 {@code canceled_at} 이 바뀌는 것은 안 본다.</b> 그것도 V5 의 답을
     * 바꾸지만, {@code CouponCancelUseService} 가 같은 트랜잭션에서
     * {@code issuances.updated_at} 을 올려 <b>발급건 축 가드가 대신 잡는다.</b>
     * {@code issuances} 를 안 건드리는 usage 정정 경로가 생기면 이 축을 넓혀야 한다.
     *
     * <p>상한은 {@code startRunStep} 이 {@link #latestUsageId()} 로 얼려 Step 문맥에 싣는다.
     * 이 EXISTS 자체는 {@code id > :maxUsageId} PK 레인지라 값싸다
     * (실측: 상한이 최신이면 0.03ms).
     */
    boolean hasUsagesAddedAbove(long frozenMaxUsageId, LocalDateTime asOf);

    /**
     * 사용 이력의 <b>절대 최대 식별자</b>. 실행 시작에 한 번 재 문맥에 얼린다.
     *
     * <p><b>{@code asOf} 로 자르지 않는다.</b> id 는 오토인크리먼트라 얼린 뒤에 들어오는
     * 행은 반드시 이 값보다 크다 — 자르지 않아도 {@link #hasUsagesAddedAbove} 의 뜻이
     * 그대로다. 자르면 {@code used_at} 인덱스가 없어 <b>132만 행 전수 스캔</b>이 되는데
     * (실측 {@code type=ALL · rows=1,313,897 · 0.32초}), 얻는 것이 없다.
     * 근거와 실행계획은 어댑터 구현에 적었다.
     *
     * <p>행이 없으면 <b>0</b> 이다. 그 값을 그대로 상한으로 쓰면
     * {@link #hasUsagesAddedAbove} 가 <i>"id &gt; 0 이면서 asOf 이하인 행이 생겼는가"</i> 가 되어
     * 뜻이 정확히 맞는다 — 형제 이력 축이 같은 이유로 같은 기본값을 쓴다.
     *
     * <p><b>V5 도 이 상한을 쓴다</b> — {@code AsOfStateRepository#applyActiveUsageCounts} 가
     * {@code id <= maxUsageId} 로 센다. 두 축이 같은 경계를 보게 하려는 것이다.
     *
     * <p>⚠️ <b>그래도 결정론이 보장되지는 않는다.</b> {@code MAX(id)} 는 커밋 경계가
     * 아니라 <b>할당 순서</b>다. 상한 이하의 id 를 이미 받은 트랜잭션이 얼린 뒤 커밋하면
     * V5 는 세는데 {@link #hasUsagesAddedAbove}({@code id > 상한})는 못 본다.
     * <b>이력 축도 같은 모양이다</b>({@link #hasHistoriesAddedAbove} vs 리플레이의
     * {@code h.id <= maxHistoryId}) — 두 축이 공유하는 한계이지 이 상한이 만든 것이 아니다.
     * 닫는 법과 비용은 {@code AsOfStateJdbcAdapter#applyActiveUsageCounts} 에 적었다.
     */
    long latestUsageId();

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

    /**
     * 배치가 쓰는 <b>핵심 테이블 중 지금 스키마에 없는 것</b>을 이름으로 돌려줍니다.
     * 전부 있으면 빈 목록입니다.
     *
     * <p><b>왜 {@link #hasCleanOnlyConstraints()} 로 대신할 수 없나.</b> 그것은 인덱스
     * <b>하나의 존재</b>를 묻는 {@code EXISTS} 라 <b>테이블이 하나도 없어도 예외 없이
     * {@code false}</b> 를 돌려줍니다. 그러면 <i>"스키마가 없다"</i> 와 <i>"CORRUPT 스키마다"</i>
     * 가 한 값으로 접힙니다 — 이 프로젝트가 반복해서 막아 온 <b>"0건이 두 뜻을 갖는다"</b> 와
     * 같은 종류입니다. 두 축을 갈라 놓아야 기동 시점에 앞의 것을 따로 잡을 수 있습니다.
     */
    List<String> missingCoreTables();

    /**
     * 지금 접속이 보고 있는 <b>스키마 이름</b>. URL 에 DB 이름이 없으면 {@code null} 입니다.
     *
     * <p>{@link #missingCoreTables()} 만으로는 <b>같은 증상이 두 원인을 갖습니다</b> —
     * 스키마를 안 만든 것과, 접속 URL 에 DB 이름을 안 준 것. 뒤의 경우
     * {@code table_schema = DATABASE()} 가 {@code NULL} 비교라 UNKNOWN 이 되어
     * <b>전부 없다</b>고 답하는데, 그때 필요한 조치는 마이그레이션이 아니라 URL 수정입니다.
     */
    String currentSchema();

    /**
     * 배치가 <b>이름으로 짚는 컬럼</b> 중 지금 스키마에 없는 것을 {@code 테이블.컬럼} 으로
     * 돌려줍니다. 전부 있으면 빈 목록입니다.
     *
     * <p>테이블이 있다고 컬럼도 있는 것은 아닙니다. {@code verification_runs.origin} 은
     * cy-seed {@code 1f217b5} 부터 생겼는데, 그 이전에 만든 검증용 셋에는 <b>테이블은 다
     * 있고 이 컬럼만 없습니다.</b> Flyway 는 그 DB 에 닿지 않아 마이그레이션으로 못 고칩니다.
     *
     * <p>그 상태로 띄우면 기동은 통과하고 되읽기가 <b>매 주기 {@code Unknown column} 으로
     * 실패</b>합니다. 게이지는 직전 값을 유지하므로 조용하고, 알림이 뜨기까지 최소 15분이
     * 걸립니다. 기동 시점에 잡으면 즉시, 그리고 조치까지 함께 말할 수 있습니다.
     */
    List<String> missingCriticalColumns();

    /**
     * 이 실행이 <b>어떤 데이터를 봤는지</b>를 한 값으로 접습니다.
     *
     * <p>계약({@code docs/contract.json} 의 {@code fingerprint})이 정한 공식을 글자 그대로 씁니다.
     *
     * <pre>
     * SHA256( max(issuance_histories.id) | count(issuance_histories) | count(issuances)
     *         | sum(coupon_stocks.active_count) | max(issuances.updated_at) )
     * 구분자 "|" · 시각 "%Y-%m-%d %H:%M:%S.%f" · 이력은 created_at &lt;= asOf
     * </pre>
     *
     * <p><b>{@code findings_checksum} 과 짝으로만 뜻이 있습니다.</b> 판정표가 그 조합을 읽습니다 —
     * 지문이 다르면 데이터가 달라진 것이고, <b>지문은 같은데 checksum 이 다르면 검증기 버그</b>입니다.
     * 이 프로젝트가 진짜로 잡고 싶은 칸이 후자입니다.
     *
     * <p><b>{@code policyDigest} 와 다른 값입니다.</b> 그쪽은 한 실행 안에서 회차 정책이
     * 안 바뀌었는지만 보는 실행 중 가드이고, 이쪽은 실행 사이를 비교하는 데이터셋 식별자입니다.
     * 그래서 이 공식에는 {@code eligible_grades_mask} 축이 없습니다 — 계약이 그렇게 정했습니다.
     */
    String datasetFingerprint(LocalDateTime asOf);

    /**
     * <b>배치 메타의 성능 인덱스 중 없는 것.</b> 테이블·컬럼 축과 달리 이쪽이 빠져도
     * <b>기동과 동작이 통과한다</b> — 되읽기가 데드라인을 넘겨 게이지가 {@code NaN} 이
     * 되거나 정리 잡이 매 청크 전체 스캔을 하는 것으로만 늦게 드러난다.
     * 그래서 기동 때 묻는다.
     *
     * <p><b>반환 형식이 계약이다.</b> {@code SchemaPresenceGuard} 가 괄호 앞을 떼어
     * 인덱스별 처방을 찾으므로, 형식을 바꾸면 그쪽이 조용히 폴백으로 떨어진다.
     *
     * @return {@code TABLE.INDEX(COL1,COL2)} 형식 — 컬럼은 인덱스 순서. 다 있으면 빈 목록.
     *         컬럼은 <b>접두사</b>로 비교하므로 뒤에 더 붙은 인덱스는 "있다" 로 본다
     */
    List<String> missingCriticalIndexes();
}
