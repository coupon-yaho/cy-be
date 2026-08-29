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

    @Query("""
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
              AND issuance.id = :issuanceId
            """)
    Optional<MemberCouponProjection> findMemberCoupon(
            @Param("memberId") Long memberId,
            @Param("issuanceId") Long issuanceId
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
     * <p><b>{@code GREATEST} 로 세 값 중 최댓값을 쓴다.</b> {@code CURRENT_TIMESTAMP(6)} 이
     * 진짜 쓰기 시각이고, 기존 값과 넘어온 값을 함께 넣어 <b>어느 경로로도 뒤로 안 물러난다.</b>
     * 만료 배치가 같은 이유로 같은 모양을 쓴다({@code ExpirationJdbcAdapter} —
     * {@code GREATEST(updated_at, :committedAt)}). 사건 시각은 이 컬럼이 아니라
     * {@code issuance_histories.created_at} 이 진다.
     *
     * <p>JPQL 을 네이티브로 내린 이유는 {@code CURRENT_TIMESTAMP} 가 MySQL 에서 <b>초 정밀도</b>
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
                updated_at = GREATEST(updated_at, :updatedAt, CURRENT_TIMESTAMP(6))
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
