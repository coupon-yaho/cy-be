package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.storage.db.coupon.entity.IssuanceEntity;

public interface IssuanceJpaRepository
        extends JpaRepository<IssuanceEntity, Long> {

    /** 회차와 회원이 같은 기존 발급건의 존재 여부를 조회합니다. */
    boolean existsByCouponIdAndMemberId(Long couponId, Long memberId);

    @Query(value = """
            SELECT issuance.*
            FROM issuances issuance
            JOIN issuance_histories history
              ON history.issuance_id = issuance.id
             AND history.event_type = 'ISSUE'
            WHERE issuance.coupon_id = :couponRoundId
              AND issuance.member_id = :memberId
              AND history.request_id = :idempotencyKey
            """, nativeQuery = true)
    Optional<IssuanceEntity> findForCouponRoundMemberAndIdempotencyKey(
            @Param("couponRoundId") Long couponRoundId,
            @Param("memberId") Long memberId,
            @Param("idempotencyKey") String idempotencyKey
    );

    @Query(
            value = """
                    SELECT issuance.id AS issuanceId,
                           issuance.couponId AS couponRoundId,
                           issuance.code AS code,
                           issuance.status AS status,
                           couponRound.name AS name,
                           couponRound.policyType AS policyType,
                           couponRound.discountRate AS discountRate,
                           couponRound.maxDiscountAmount AS maxDiscountAmount,
                           couponRound.discountAmount AS discountAmount,
                           issuance.issuedAt AS issuedAt,
                           issuance.expiresAt AS expiresAt,
                           activeUsage.usedAt AS usedAt,
                           activeUsage.discountAmount AS usedDiscountAmount,
                           activeUsage.orderId AS orderId
                    FROM IssuanceEntity issuance
                    JOIN CouponRoundEntity couponRound
                      ON couponRound.id = issuance.couponId
                    LEFT JOIN IssuanceUsageEntity activeUsage
                      ON activeUsage.issuanceId = issuance.id
                     AND activeUsage.canceledAt IS NULL
                    WHERE issuance.memberId = :memberId
                      AND (:status IS NULL OR issuance.status = :status)
                    ORDER BY issuance.issuedAt DESC, issuance.id DESC
                    """,
            countQuery = """
                    SELECT COUNT(issuance.id)
                    FROM IssuanceEntity issuance
                    WHERE issuance.memberId = :memberId
                      AND (:status IS NULL OR issuance.status = :status)
                    """
    )
    Page<MemberCouponProjection> findMemberCoupons(
            @Param("memberId") Long memberId,
            @Param("status") IssuanceStatus status,
            Pageable pageable
    );

    /**
     * <b>{@code updated_at} 은 요청 시각이 아니라 쓰기 시각이다.</b> 검증의 얼림 가드
     * ({@code assertFrozenStep} → {@code hasIssuancesUpdatedAfter} ·
     * {@code hasStocksUpdatedAfter})가 <i>"실행이 도는 동안 이 축이 얼어 있었다"</i> 를
     * 이 컬럼 하나로 관측한다 — <b>쓰는 쪽이 시각을 뒤로 밀 수 있으면 그것은 관측 수단이
     * 아니다.</b>
     *
     * <p><b>뒤로 밀리는 경로가 실재했다.</b> 넘어오는 시각은 {@code IdempotencyExecutionService}
     * 가 <b>멱등 선점 이전에</b> 잡은 {@code requestAt} 이라, 재고 행 락을 기다린 시간이
     * 통째로 값과 커밋 사이의 간격이 된다. D5 부하(2만 동시)에서는 그 대기가 곧 백데이트
     * 폭이다 — 검증이 {@code asOf = T} 로 시작한 뒤 {@code T-800ms} 로 선점한 요청이
     * {@code T+5s} 에 커밋하면 가드가 그 변경을 <b>못 본다.</b>
     *
     * <p><b>{@code GREATEST} 로 세 값 중 최댓값을 쓴다.</b> 기존 값과 넘어온 값을 함께 넣어
     * <b>어느 경로로도 뒤로 안 물러난다.</b> 만료 배치가 같은 이유로 같은 모양을 쓴다
     * ({@code ExpirationJdbcAdapter} — {@code GREATEST(updated_at, :committedAt)}).
     * 사건 시각은 이 컬럼이 아니라 {@code issuance_histories.created_at} 이 진다.
     *
     * <p>⚠️ <b>{@code UTC_TIMESTAMP(6)} 은 "커밋 시각" 이 아니다 — 문장 시작에 한 번
     * 고정되고 행 락 대기를 안 따라간다.</b> 실측(MySQL 8.4):
     *
     * <pre>
     *   문장 시작 직전  13:31:19.269270
     *   행에 찍힌 값    13:31:19.269371   ← 락을 2초 기다렸는데 시작 시각이다
     *   문장 끝난 뒤    13:31:21.255055
     * </pre>
     *
     * <b>그래서 창이 0 이 되지는 않는다.</b> 줄어들 뿐이다 — 이전에는 <b>멱등 선점 이전</b>
     * (트랜잭션 시작 전, 재고 락 대기까지 전부 포함)이었고 지금은 <b>이 UPDATE 문장 시작</b>
     * (재고 락 대기 뒤)이다. 남는 것은 <i>"{@code issuances} 행 락 대기 + 커밋 구간"</i> 이다.
     *
     * <p><b>타임스탬프로는 여기까지가 끝이다.</b> 문장 안에서 평가되는 값은 언제나 커밋보다
     * 앞서므로, <i>"asOf 전에 시작해 asOf 후에 커밋한 트랜잭션"</i> 은 어떤 시각 함수로도
     * 못 잡는다. 그 잔여를 정말 닫으려면 커밋 순서를 보는 축(binlog·GTID)이 필요하고
     * 그것은 이 티켓 밖이다. 검증 쪽은 시작의 {@code rejectIssuancesUpdatedAfterAsOf} 와
     * 끝의 {@code assertStillFrozen} 두 겹으로 창을 더 좁힌다.
     *
     * <p>JPQL 을 네이티브로 내린 이유는 {@code UTC_TIMESTAMP} 가 MySQL 에서 <b>초 정밀도</b>
     * 라서다. 이 컬럼은 {@code datetime(6)} 이고 리플레이가 마이크로초까지 본다.
     *
     * <p>⚠️ <b>상태는 {@code String} 으로 받는다.</b> 네이티브 질의라 {@code @Enumerated} 변환이
     * 안 걸린다 — 어댑터가 {@code name()} 을 넘긴다. SpEL({@code :#{#x.name()}})로 감추지
     * 않는 이유는 이 저장소에 그 선례가 없고, 바인딩이 조용히 어긋나면 <b>조건이 안 맞아
     * 0행</b>이 되어 "그 사이 누가 상태를 바꿨다" 로 잘못 읽히기 때문이다.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE issuances
            SET status = :nextStatus,
                updated_at = GREATEST(updated_at, :updatedAt, UTC_TIMESTAMP(6))
            WHERE id = :issuanceId
              AND member_id = :memberId
              AND status = :currentStatus
            """, nativeQuery = true)
    int updateStatusIfCurrent(
            @Param("issuanceId") Long issuanceId,
            @Param("memberId") Long memberId,
            @Param("currentStatus") String currentStatus,
            @Param("nextStatus") String nextStatus,
            @Param("updatedAt") Instant updatedAt
    );
}
