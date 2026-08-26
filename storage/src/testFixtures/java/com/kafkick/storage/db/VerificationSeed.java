package com.kafkick.storage.db;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;

/**
 * issuances 는 coupons·members 를, coupons 는 coupon_templates·brands 를, members 는 grades 를
 * FK 로 문다. 사용 행 하나를 넣으려면 이 사슬을 전부 세워야 한다.
 *
 * <p>검증하려는 값과 무관한 컬럼은 고정값으로 채운다. 회차·회원은 처음 필요할 때 한 번만 만든다.
 */
public final class VerificationSeed {

    /**
     * 발급 시각의 기본값. <b>저장소 안 모든 테스트의 {@code asOf} 보다 앞서야 한다</b> —
     * V3 는 {@code updated_at <= asOf} 인 발급건만 비교하므로 뒤에 있으면 통째로 빠진다.
     *
     * <p>{@code issued_at}·{@code updated_at}·{@code expires_at} 을 <b>한 기준점에서 파생</b>시킨다.
     * 상수를 따로 두고 하나만 과거로 밀면 "마지막 상태 변경이 발급보다 먼저" 같은,
     * 런타임이 만들 수 없는 데이터가 된다. 시각을 보는 규칙이 하나 늘 때마다 다시 깨진다.
     */
    private static final LocalDateTime DEFAULT_ISSUED_AT = LocalDateTime.of(2025, 1, 1, 0, 0);

    /** 자식이 먼저다. 순서가 틀리면 FK 가 삭제를 거부한다. */
    private static final List<String> TABLES_IN_DELETE_ORDER = List.of(
            // expected_findings 는 FK 가 없어 DELETE 가 막히지는 않지만 uk_expected 가 있어,
            // 행이 새면 다음 테스트가 같은 seed_run_id 로 심다가 중복키로 죽는다.
            // 통계 셋은 아직 아무도 안 채우지만 verification_runs 를 FK 로 문다.
            // 통계 Step 이 붙는 순간 이 목록이 없으면 DELETE 가 막혀,
            // 원인 테스트가 아니라 그다음 테스트가 빨개진다.
            "hourly_stats", "grade_stats", "coupon_stats",
            "asof_state", "verification_findings", "expected_findings", "verification_runs",
            "idempotency_records",
            "issuance_usages", "issuance_histories", "issuances",
            "coupon_stocks", "coupons", "coupon_templates", "brands",
            "members", "grades");

    private final JdbcClient jdbcClient;

    private Long couponId;
    private boolean gradesInserted;
    private int codeSequence;
    private String lastCode;

    public VerificationSeed(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /** 발급건 하나를 만들고 식별자를 돌려준다. */
    public long issuance(IssuanceStatus status) {
        return issuance(status, DEFAULT_ISSUED_AT);
    }

    /** 발급 시각을 지정한다. {@code updated_at} 도 같은 값으로 둔다 — 둘이 어긋난 발급건은 없다. */
    public long issuance(IssuanceStatus status, LocalDateTime issuedAt) {
        long issuanceId = insertIssuance(status, issuedAt, "VIP");
        syncStock(status, issuedAt);
        return issuanceId;
    }

    private long insertIssuance(IssuanceStatus status, LocalDateTime issuedAt, String issuedGrade) {
        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO issuances
                            (coupon_id, member_id, code, issued_grade, status,
                             issued_at, expires_at, updated_at)
                        VALUES (:couponId, :memberId, :code, :issuedGrade, :status,
                                :issuedAt, :expiresAt, :updatedAt)
                        """)
                .param("issuedGrade", issuedGrade)
                .param("couponId", couponId())
                .param("memberId", newMemberId())
                .param("code", lastCode = nextCode())
                .param("status", status.name())
                .param("issuedAt", issuedAt)
                .param("expiresAt", issuedAt.plusDays(7))
                .param("updatedAt", issuedAt));
    }

    /**
     * 발급 하나마다 재고를 함께 맞춘다.
     *
     * <p><b>기본 시드는 정합한 CLEAN 셋이어야 한다.</b> 운영에서 회차와 재고는 1:1 로 함께
     * 움직이므로, 재고 행 없이 발급만 쌓인 상태는 그 자체가 V1 검출 대상이다.
     * 시드가 그런 데이터를 기본으로 만들면 모든 테스트가 V1 을 침묵시키는 코드를 들고 다녀야 한다.
     *
     * <p>시각도 발급 시각을 쓴다. {@code DEFAULT_ISSUED_AT} 을 박으면
     * {@code issuance(status, 2026-01-15)} 을 부른 순간 <b>재고가 줄기 전에 발급이 있었다</b>는,
     * 런타임이 만들 수 없는 데이터가 된다 — 이 파일이 발급건 시각에 대해 이미 금지한 형태다.
     * 갱신 경로는 {@code GREATEST} 로 민다. 재고 시각은 마지막 변경이라 뒤로 물러나면 안 된다.
     *
     * <p>맞추는 기준은 <b>저장된 status</b> 다. 이력으로 접힌 상태를 일부러 다르게 세운 테스트는
     * 재고 축이 자동으로 맞지 않으므로 {@link #overwriteStock(int)} 로 직접 맞춰야 한다 —
     * 그 어긋남을 잡는 것이 V1 이기 때문이다.
     *
     * <p>V1 을 울리고 싶은 테스트는 {@link #overwriteStock(int)} 로 덮어쓰거나
     * {@link #removeStock()} 로 지운다.
     */
    private void syncStock(IssuanceStatus status, LocalDateTime issuedAt) {
        // 행은 항상 만든다. CANCELLED·EXPIRED 만 있는 회차도 운영이라면 active_count 0 인 행이
        // 있다. 안 만들면 V1 이 침묵하는 이유가 "0 == 0" 이 아니라 "양쪽 다 없어서" 가 되고,
        // coupons 를 드라이빙으로 고른 근거(재고 행 없는 회차를 잡는다)와 반대되는 데이터가 깔린다.
        int delta = status.countsTowardStock() ? 1 : 0;

        jdbcClient.sql("""
                        INSERT INTO coupon_stocks (coupon_id, total_quantity, active_count, updated_at)
                        VALUES (:couponId, 100, :delta, :updatedAt) AS incoming
                        ON DUPLICATE KEY UPDATE
                            active_count = coupon_stocks.active_count + :delta,
                            updated_at   = GREATEST(coupon_stocks.updated_at, incoming.updated_at)
                        """)
                .param("couponId", couponId())
                .param("delta", delta)
                .param("updatedAt", issuedAt)
                .update();
    }

    /** 재고 행을 지운다. "재고 없이 발급만 쌓인" 상태를 만드는 유일한 수단이다. */
    public void removeStock() {
        jdbcClient.sql("DELETE FROM coupon_stocks WHERE coupon_id = :couponId")
                .param("couponId", couponId())
                .update();
    }

    /** 이력 한 행을 만들고 식별자를 돌려준다. {@code fromStatus} 가 null 이면 발급 이력이다. */
    public long history(
            long issuanceId,
            IssuanceEventType eventType,
            IssuanceStatus fromStatus,
            IssuanceStatus toStatus,
            LocalDateTime createdAt
    ) {
        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO issuance_histories
                            (issuance_id, event_type, from_status, to_status, created_at)
                        VALUES (:issuanceId, :eventType, :fromStatus, :toStatus, :createdAt)
                        """)
                .param("issuanceId", issuanceId)
                .param("eventType", eventType.name())
                .param("fromStatus", fromStatus == null ? null : fromStatus.name())
                .param("toStatus", toStatus.name())
                .param("createdAt", createdAt));
    }

    /** 사용 행 하나. {@code canceledAt} 이 null 이면 취소되지 않은 사용이다. */
    public void usage(long issuanceId, LocalDateTime usedAt, LocalDateTime canceledAt) {
        jdbcClient.sql("""
                        INSERT INTO issuance_usages
                            (issuance_id, order_id, discount_amount, used_at, canceled_at)
                        VALUES (:issuanceId, NULL, 1000, :usedAt, :canceledAt)
                        """)
                .param("issuanceId", issuanceId)
                .param("usedAt", usedAt)
                .param("canceledAt", canceledAt)
                .update();
    }

    /**
     * 발급 시점 등급을 지정한다. V6 는 이 스냅샷과 회차 마스크만 봐서 판정한다 —
     * {@code members.membership_grade} 는 조인하지 않으므로 회원 등급과 달라도 된다.
     *
     * <p>{@code grades} 에 없는 문자열도 넣을 수 있다. 그 자체가 위반이라 검사 대상이다.
     */
    public long issuance(IssuanceStatus status, String issuedGrade) {
        long issuanceId = insertIssuance(status, DEFAULT_ISSUED_AT, issuedGrade);
        syncStock(status, DEFAULT_ISSUED_AT);
        return issuanceId;
    }

    /**
     * <b>접힌 상태 기준으로 재고를 맞춘다.</b> 저장 {@code status} 와 접기를 일부러 다르게 세운
     * 테스트가 V1 을 침묵시키는 수단이다 — {@code overwriteStock} 과 같은 일을 하지만
     * 이름이 <b>무엇에 맞추는지</b>를 말한다.
     *
     * <p>V1 이 보는 것은 {@code asof_state.state}(접힌 값)이고 {@code syncStock} 이 맞추는 것은
     * {@code issuances.status}(저장값)이다. 둘이 다른 테스트 — V3·V4 테스트의 본질이 그렇다 —
     * 에서는 <b>반드시 어긋나고</b>, 작성자가 의도하지 않은 {@code STOCK_MISMATCH} 가 하나 더 나온다.
     */
    public void matchStockToReplay(int activeAfterReplay) {
        overwriteStock(activeAfterReplay);
    }

    /**
     * 회차의 재고를 이 값으로 <b>확정</b>한다 — {@code issuance()} 의 누적을 무시하고 덮어쓴다.
     *
     * <p>{@code issuance()} 가 저장 status 기준으로 이미 맞추므로 기본 시드는 정합하다.
     * 이 메서드는 그것을 덮어써 V1 을 울리거나, 이력으로 접힌 상태를 일부러 다르게 세운
     * 테스트에서 재고 축을 그쪽에 맞출 때 쓴다.
     */
    public void overwriteStock(int activeCount) {
        overwriteStock(activeCount, DEFAULT_ISSUED_AT);
    }

    /**
     * {@code updated_at} 도 지정한다. 재고 축 가드와 {@code updated_at <= asOf} 경계를 보는 데 쓴다.
     *
     * <p><b>{@code syncStock} 의 {@code GREATEST} 규약을 깨고 덮어쓴다.</b> 그쪽은 발급을 쌓는
     * 자리라 시각이 뒤로 물러나면 안 되지만, 여기는 <b>시각 경계를 검사하는 테스트가 부르는 자리</b>다.
     * {@code GREATEST} 를 쓰면 {@code DEFAULT_ISSUED_AT} 보다 이른 시각을 넘겼을 때 조용히 무시되고,
     * {@code hasStocksUpdatedAfter} 경계나 {@code V4} 의 소수 초 회귀를 검사하려던 테스트가
     * <b>다른 값을 보고도 초록</b>이 된다.
     */
    public void overwriteStock(int activeCount, LocalDateTime updatedAt) {
        jdbcClient.sql("""
                        INSERT INTO coupon_stocks (coupon_id, total_quantity, active_count, updated_at)
                        VALUES (:couponId, 100, :activeCount, :updatedAt) AS incoming
                        ON DUPLICATE KEY UPDATE
                            active_count = incoming.active_count,
                            updated_at   = incoming.updated_at
                        """)
                .param("couponId", couponId())
                .param("activeCount", activeCount)
                .param("updatedAt", updatedAt)
                .update();
    }

    /**
     * 회차의 허용 등급 마스크를 좁힌다. 기본은 15(WELCOME+SILVER+GOLD+VIP 전부)라
     * 그대로 두면 V6 가 절대 울지 않는다.
     *
     * @param mask 비트합. WELCOME 1 · SILVER 2 · GOLD 4 · VIP 8
     */
    public void restrictCouponTo(int mask) {
        jdbcClient.sql("UPDATE coupons SET eligible_grades_mask = :mask WHERE id = :couponId")
                .param("mask", mask)
                .param("couponId", couponId())
                .update();
    }

    /**
     * 회차를 새로 만들고 이후 {@code issuance()}·{@code overwriteStock()}·{@code restrictCouponTo()} 가
     * 그것을 가리키게 한다.
     *
     * <p><b>회차가 하나뿐이면 귀속을 검증할 수 없다.</b> {@code GROUP BY i.coupon_id} 를 지우고
     * 전체 합계를 내도 결과가 같아서 통과한다. 회차 그레인 규칙에서 귀속이 밀리면
     * 개수는 맞고 키만 달라져 <b>누락 N · 오탐 N 이 동시에</b> 뜬다.
     */
    public long newCoupon() {
        long brandId = insertBrand();
        couponId = insertCoupon(insertTemplate(brandId), brandId);
        return couponId;
    }

    /**
     * <b>상태와 시각을 직접 주는 회차.</b> 회차 상태 전이(CY-446)를 재려면
     * {@code SCHEDULED} 와 임의의 {@code open_at}·{@code close_at} 이 필요한데,
     * {@link #newCoupon()} 은 {@code OPEN} 과 고정 시각을 박는다 — 그쪽을 고치면 이 시드를
     * 쓰는 다른 테스트 전부의 전제가 바뀐다.
     *
     * <p>재고 행을 함께 만든다. {@code coupon_stocks} 에 행이 없는 회차는 전이 대상에서
     * 일부러 빠지므로, <b>재고 없는 경우를 재려면</b> {@link #roundWithoutStock} 을 쓴다.
     *
     * <p>이 회차를 {@code couponId} 로 삼지 <b>않는다</b>. 전이 테스트는 회차를 여럿 심으므로
     * "현재 회차" 라는 개념이 없고, 그것을 밀면 다른 시드 메서드의 전제가 흔들린다.
     */
    public long round(CouponStatus status, LocalDateTime openAt, LocalDateTime closeAt) {
        long id = insertRound(status, openAt, closeAt);
        jdbcClient.sql("""
                        INSERT INTO coupon_stocks (coupon_id, total_quantity, active_count, updated_at)
                        VALUES (:couponId, 100, 0, :updatedAt)
                        """)
                .param("couponId", id)
                .param("updatedAt", openAt)
                .update();
        return id;
    }

    /**
     * <b>재고 행이 없는 회차.</b> FK({@code V1:635})는 재고 행이 회차를 가리키는 방향이라
     * <b>회차마다 재고 행이 있음을 강제하지 않는다</b> — 그래서 실재할 수 있는 상태다. 이 회차를 열면 발급 경로가 그 회차에서 죽으므로,
     * 전이가 일부러 안 여는 것을 재는 데 쓴다.
     */
    public long roundWithoutStock(CouponStatus status, LocalDateTime openAt,
            LocalDateTime closeAt) {
        return insertRound(status, openAt, closeAt);
    }

    private long insertRound(CouponStatus status, LocalDateTime openAt, LocalDateTime closeAt) {
        long brandId = insertBrand();
        long templateId = insertTemplate(brandId);
        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO coupons
                            (template_id, brand_id, name, policy_type, discount_amount,
                             valid_days, eligible_grades_mask, open_at, close_at, status, created_at)
                        VALUES (:templateId, :brandId, '전이테스트회차', 'FIXED_AMOUNT', 1000,
                                7, 15, :openAt, :closeAt, :status, :createdAt)
                        """)
                .param("templateId", templateId)
                .param("brandId", brandId)
                .param("openAt", openAt)
                .param("closeAt", closeAt)
                .param("status", status.name())
                .param("createdAt", openAt.minusDays(1)));
    }

    /**
     * 없으면 만든다. 회차 자체가 필요할 뿐인 자리에서 쓴다.
     *
     * <p><b>단언 안에서는 쓰지 마라.</b> 그 자리에서 회차가 새로 생기면
     * {@code currentCouponId()} 가 막으려던 사고가 그대로 난다.
     */
    public long currentCouponIdOrCreate() {
        return couponId();
    }

    /**
     * 이 시드가 만든 회차의 식별자. V1 검출의 {@code target_key} 대조에 쓴다.
     *
     * <p><b>만들지 않는다.</b> 조회처럼 보이는데 회차를 만들면, 단언 안에서 부를 때
     * 그 자리에서 새 회차가 생겨 <b>"검출이 엉뚱한 회차를 가리킨다"</b> 는 실패 메시지를 낸다 —
     * 실제 원인은 단언 자신이다. 원인을 찾는 데 가장 오래 걸리는 형태다.
     */
    public long currentCouponId() {
        if (couponId == null) {
            throw new IllegalStateException(
                    "회차가 아직 없습니다. issuance()·overwriteStock()·newCoupon() 중 하나를 먼저 부르십시오.");
        }
        return couponId;
    }

    /**
     * 검증이 건드리는 테이블을 FK 역순으로 비운다.
     *
     * <p>{@code @RepositoryTest} 는 테스트마다 롤백하므로 부를 일이 없다.
     * 잡을 실제로 돌리는 테스트는 트랜잭션 밖이라 롤백이 없어 이걸 써야 한다.
     */
    public void clear() {
        TABLES_IN_DELETE_ORDER.forEach(
                table -> jdbcClient.sql("DELETE FROM " + table).update());

        // **배치 메타도 함께 비운다.** 컨테이너를 JVM 이 공유하게 되면서(MySqlContainerConfig)
        // BATCH_* 가 클래스 경계를 넘어 살고, 남은 JobInstance 가 다음 클래스의 같은 asOf 와
        // 충돌해 JobInstanceAlreadyCompleteException 을 낸다 — 실측: 잡을 돌리는 배치 테스트
        // 29개 중 25개가 같은 asOf(2026-01-15T09:00)를 쓴다.
        //
        // 한때 두 클래스만 BatchMetadata.clear() 를 부르고 나머지는 removeJobExecutions() 에
        // 기댔는데, 그것은 **실행이 없는 인스턴스를 남긴다**(바이트코드 확인). 강도가 두 벌이면
        // "왜 저기만 지우지" 를 다음 사람이 판단해야 하고, 판단이 틀리면 순서 의존 초록이 생긴다.
        //
        // 여기 두는 이유는 **이미 데이터를 비우는 자리**여서다. 부르는 쪽이 늘어날 필요가 없다.
        BatchMetadata.clear(jdbcClient);

        // 캐시를 안 비우면 다음 issuance() 가 방금 지운 회차·등급을 FK 로 가리킨다.
        couponId = null;
        gradesInserted = false;
        // 지운 발급의 code 를 복제 원본으로 집으면 "복제했다고 믿는데 실제로는 정상 데이터" 가 된다.
        lastCode = null;
        codeSequence = 0;
    }

    private long couponId() {
        return couponId == null ? newCoupon() : couponId;
    }

    // ─────────────────────────── V2 · CORRUPT 전용 ───────────────────────────

    /**
     * 발급 하나를 새 회원으로 만들고 <b>그 회원 식별자</b>를 돌려준다. V2 는 회원 단위 규칙이라
     * {@code issuance()} 가 돌려주는 발급건 식별자로는 검출 키를 만들 수 없다.
     *
     * <p><b>{@code CorruptRepositoryTest} 에서만 의미가 있다.</b> 아래 셋은 CLEAN 에서
     * {@code uk_coupon_member} 와 {@code issuances.code} UNIQUE 에 막혀 INSERT 자체가 튕긴다.
     */
    public long issuanceForNewMember() {
        long memberId = newMemberId();
        insertIssuanceFor(memberId, nextCode(), DEFAULT_ISSUED_AT);
        return memberId;
    }

    /** 이미 있는 회원에게 같은 회차로 한 건 더 — 오염 유형 6 의 모양이다. */
    public void issuanceForMember(long memberId) {
        issuanceForMember(memberId, DEFAULT_ISSUED_AT);
    }

    /** 시각을 지정한다. {@code asOf} 경계를 넘는 두 번째 발급을 세우는 데 쓴다. */
    public void issuanceForMember(long memberId, LocalDateTime updatedAt) {
        insertIssuanceFor(memberId, nextCode(), updatedAt);
    }

    /** 이미 있는 회원에게 같은 {@code code} 로 한 건 더 — 두 케이스에 동시에 걸리는 모양이다. */
    public void issuanceForMemberWithCode(long memberId, String code) {
        insertIssuanceFor(memberId, code, DEFAULT_ISSUED_AT);
    }

    /** 같은 {@code code} 를 새 회원에게 복제 — 오염 유형 5 의 모양이다. 회원 식별자를 돌려준다. */
    public long issuanceForNewMemberWithCode(String code) {
        return issuanceForNewMemberWithCode(code, DEFAULT_ISSUED_AT);
    }

    /** 시각을 지정한다. 케이스 2 의 {@code asOf} 경계를 세우는 데 쓴다. */
    public long issuanceForNewMemberWithCode(String code, LocalDateTime updatedAt) {
        long memberId = newMemberId();
        insertIssuanceFor(memberId, code, updatedAt);
        return memberId;
    }

    /** 마지막으로 심은 발급의 code. 복제할 원본을 집을 때 쓴다. */
    public String currentIssuanceCode() {
        if (lastCode == null) {
            throw new IllegalStateException(
                    "발급이 아직 없습니다. issuanceForNewMember() 를 먼저 부르십시오.");
        }
        return lastCode;
    }

    /**
     * <b>이력과 재고까지 함께 만든다.</b> 발급 행만 넣으면 접기 결과가 0 이고 재고도 0 이라
     * V1 이 <b>"둘 다 없어서" 조용할 뿐</b>인 상태가 된다 — 이 파일이 금지한 형태다.
     * 그 상태에 누가 이력을 한 줄 넣는 순간 재고만 어긋나 V2 테스트가
     * 엉뚱한 {@code STOCK_MISMATCH} 로 실패한다.
     */
    private long insertIssuanceFor(long memberId, String code, LocalDateTime updatedAt) {
        lastCode = code;
        long issuanceId = insertGenerated(jdbcClient.sql("""
                        INSERT INTO issuances
                            (coupon_id, member_id, code, issued_grade, status,
                             issued_at, expires_at, updated_at)
                        VALUES (:couponId, :memberId, :code, 'VIP', 'ISSUED',
                                :issuedAt, :expiresAt, :updatedAt)
                        """)
                .param("couponId", couponId())
                .param("memberId", memberId)
                .param("code", code)
                .param("issuedAt", DEFAULT_ISSUED_AT)
                .param("expiresAt", DEFAULT_ISSUED_AT.plusDays(7))
                .param("updatedAt", updatedAt));

        history(issuanceId, IssuanceEventType.ISSUE, null, IssuanceStatus.ISSUED, DEFAULT_ISSUED_AT);
        syncStock(IssuanceStatus.ISSUED, DEFAULT_ISSUED_AT);
        return issuanceId;
    }

    /**
     * 발급건마다 회원을 새로 만든다. CLEAN 의 {@code uk_coupon_member} 가 1인 1매를 강제하므로,
     * 같은 회차에 같은 회원으로 두 건을 넣으려면 {@code CorruptRepositoryTest} 위여야 한다.
     */
    private long newMemberId() {
        if (!gradesInserted) {
            jdbcClient.sql("""
                    INSERT IGNORE INTO grades (code, bit_value)
                    VALUES ('WELCOME', 1), ('SILVER', 2), ('GOLD', 4), ('VIP', 8)
                    """).update();
            gradesInserted = true;
        }

        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO members (membership_grade, created_at)
                        VALUES ('VIP', :createdAt)
                        """)
                .param("createdAt", DEFAULT_ISSUED_AT));
    }

    private long insertBrand() {
        return insertGenerated(jdbcClient.sql("""
                INSERT INTO brands (name, category) VALUES ('테스트브랜드', '카페')
                """));
    }

    private long insertTemplate(long brandId) {
        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO coupon_templates
                            (brand_id, name, policy_type, discount_amount, valid_days,
                             stock_per_occurrence, eligible_grades_mask, active)
                        VALUES (:brandId, '테스트템플릿', 'FIXED_AMOUNT', 1000, 7, 100, 15, true)
                        """)
                .param("brandId", brandId));
    }

    private long insertCoupon(long templateId, long brandId) {
        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO coupons
                            (template_id, brand_id, name, policy_type, discount_amount,
                             valid_days, eligible_grades_mask, open_at, close_at, status, created_at)
                        VALUES (:templateId, :brandId, '테스트회차', 'FIXED_AMOUNT', 1000,
                                7, 15, :openAt, :closeAt, 'OPEN', :createdAt)
                        """)
                .param("templateId", templateId)
                .param("brandId", brandId)
                .param("openAt", DEFAULT_ISSUED_AT)
                .param("closeAt", DEFAULT_ISSUED_AT.plusDays(1))
                .param("createdAt", DEFAULT_ISSUED_AT));
    }

    private String nextCode() {
        return String.format("TEST%012d", ++codeSequence);
    }

    private static long insertGenerated(JdbcClient.StatementSpec spec) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        spec.update(keyHolder);

        Number generated = keyHolder.getKey();
        if (generated == null) {
            throw new IllegalStateException("식별자가 생성되지 않았습니다.");
        }
        return generated.longValue();
    }
}
