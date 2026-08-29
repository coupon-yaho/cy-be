package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import jakarta.persistence.QueryHint;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.storage.db.coupon.entity.CouponRoundEntity;

public interface CouponRoundJpaRepository
        extends JpaRepository<CouponRoundEntity, Long> {

    /**
     * V2 정의 목록. <b>결과가 요청 시각에 종속되지 않는 것이 계약이다.</b>
     *
     * <p>예전에는 {@code close_at > :asOf} 로 한 번 더 좁혔는데, 그 결과가 회원 축 없는 단일
     * 캐시 키({@code ALL}) 하나에 담긴다. 시점으로 좁힌 값을 시점 없는 키에 넣으면 <b>누가 먼저
     * 캐시를 채웠는지가 답을 바꾼다</b> — 늦은 요청이 먼저 채우면, 그 사이 닫힌 회차가 더 이른
     * 요청의 목록에서 통째로 사라진다. 인스턴스 시계가 어긋나면 L2 를 건너 같은 일이 벌어진다.
     *
     * <p>지금은 닫히지 않은 회차 전부를 그대로 담고, 시각 판정은 응답 직전 한 곳에서만 한다.
     * 대신 이 집합의 크기는 batch 가 {@code CLOSED} 로 넘기는 속도에 매인다.
     */
    @Query(value = """
            SELECT coupon.id AS couponRoundId,
                   coupon.valid_days AS validDays,
                   coupon.issuance_engine_version AS engineVersion
            FROM coupons coupon
            WHERE coupon.id = :couponRoundId
            """, nativeQuery = true)
    Optional<CouponRoundIssuanceDefinitionProjection> findIssuanceDefinitionById(
            @Param("couponRoundId") Long couponRoundId
    );

    @Modifying(flushAutomatically = true)
    @Query(value = """
            UPDATE coupons
            SET issuance_engine_locked = TRUE
            WHERE id = :couponRoundId
            """, nativeQuery = true)
    int lockIssuanceEngine(@Param("couponRoundId") Long couponRoundId);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE coupons
            SET issuance_engine_version = :engineVersion
            WHERE id = :couponRoundId
              AND issuance_engine_locked = FALSE
              AND status <> 'OPEN'
              AND (NOW(6) < open_at OR NOW(6) >= close_at)
            """, nativeQuery = true)
    int updateIssuanceEngineWhenNotOpen(
            @Param("couponRoundId") Long couponRoundId,
            @Param("engineVersion") String engineVersion
    );

    boolean existsByTemplateIdAndOpenAt(Long templateId, Instant openAt);

    /**
     * V2 정의 목록. <b>결과가 요청 시각에 종속되지 않는 것이 계약이다.</b>
     *
     * <p>예전에는 {@code close_at > :asOf} 로 한 번 더 좁혔는데, 그 결과가 회원 축 없는 단일
     * 캐시 키({@code ALL}) 하나에 담긴다. 시점으로 좁힌 값을 시점 없는 키에 넣으면 <b>누가 먼저
     * 캐시를 채웠는지가 답을 바꾼다</b> — 늦은 요청이 먼저 채우면, 그 사이 닫힌 회차가 더 이른
     * 요청의 목록에서 통째로 사라진다. 인스턴스 시계가 어긋나면 L2 를 건너 같은 일이 벌어진다.
     *
     * <p>지금은 닫히지 않은 회차 전부를 그대로 담고, 시각 판정은 응답 직전 한 곳에서만 한다.
     * 대신 이 집합의 크기는 batch 가 {@code CLOSED} 로 넘기는 속도에 매인다.
     */
    @Query(value = """
            SELECT coupon.id AS couponRoundId,
                   coupon.template_id AS templateId,
                   coupon.brand_id AS brandId,
                   coupon.name AS name,
                   coupon.policy_type AS policyType,
                   coupon.discount_rate AS discountRate,
                   coupon.max_discount_amount AS maxDiscountAmount,
                   coupon.discount_amount AS discountAmount,
                   coupon.valid_days AS validDays,
                   coupon.eligible_grades_mask AS eligibleGradesMask,
                   coupon.open_at AS openAt,
                   coupon.close_at AS closeAt,
                   coupon.status AS status,
                   stock.total_quantity AS totalQuantity,
                   stock.total_quantity - stock.active_count
                       AS remainingQuantity
              FROM coupons coupon
              JOIN coupon_stocks stock
                ON stock.coupon_id = coupon.id
             WHERE coupon.id = :couponRoundId
            """, nativeQuery = true)
    Optional<CouponRoundDetailProjection> findCouponRoundDetailById(
            @Param("couponRoundId") Long couponRoundId
    );

    /**
     * 회차 정책과 1인 1매 여부를 한 번의 왕복으로 읽습니다.
     *
     * <p>회차는 PK const 접근, 존재 확인은 {@code uk_coupon_member} 한 건 확인으로 처리됩니다.
     * 인덱스 작업량은 두 쿼리로 나눴을 때와 같고 왕복만 하나 줄어듭니다.
     */
    /**
     * V2 정의 목록. <b>결과가 요청 시각에 종속되지 않는 것이 계약이다.</b>
     *
     * <p>예전에는 {@code close_at > :asOf} 로 한 번 더 좁혔는데, 그 결과가 회원 축 없는 단일
     * 캐시 키({@code ALL}) 하나에 담긴다. 시점으로 좁힌 값을 시점 없는 키에 넣으면 <b>누가 먼저
     * 캐시를 채웠는지가 답을 바꾼다</b> — 늦은 요청이 먼저 채우면, 그 사이 닫힌 회차가 더 이른
     * 요청의 목록에서 통째로 사라진다. 인스턴스 시계가 어긋나면 L2 를 건너 같은 일이 벌어진다.
     *
     * <p>지금은 닫히지 않은 회차 전부를 그대로 담고, 시각 판정은 응답 직전 한 곳에서만 한다.
     * 대신 이 집합의 크기는 batch 가 {@code CLOSED} 로 넘기는 속도에 매인다.
     */
    @Query(value = """
            SELECT coupon.id AS couponRoundId,
                   coupon.template_id AS templateId,
                   coupon.brand_id AS brandId,
                   coupon.name AS name,
                   coupon.policy_type AS policyType,
                   coupon.discount_rate AS discountRate,
                   coupon.max_discount_amount AS maxDiscountAmount,
                   coupon.discount_amount AS discountAmount,
                   coupon.valid_days AS validDays,
                   coupon.eligible_grades_mask AS eligibleGradesMask,
                   coupon.open_at AS openAt,
                   coupon.close_at AS closeAt,
                   coupon.status AS status,
                   coupon.generated_at AS generatedAt,
                   EXISTS (
                         SELECT 1
                         FROM issuances issuance
                         WHERE issuance.coupon_id = coupon.id
                           AND issuance.member_id = :memberId
                   ) AS alreadyIssued
              FROM coupons coupon
             WHERE coupon.id = :couponRoundId
            """, nativeQuery = true)
    Optional<CouponIssuePolicyProjection> findIssuePolicySnapshot(
            @Param("couponRoundId") Long couponRoundId,
            @Param("memberId") Long memberId
    );

    @Query(
            value = """
                    SELECT coupon.id AS couponRoundId,
                           coupon.template_id AS templateId,
                           coupon.brand_id AS brandId,
                           coupon.name AS name,
                           coupon.policy_type AS policyType,
                           coupon.discount_rate AS discountRate,
                           coupon.max_discount_amount AS maxDiscountAmount,
                           coupon.discount_amount AS discountAmount,
                           coupon.valid_days AS validDays,
                           coupon.eligible_grades_mask AS eligibleGradesMask,
                           coupon.open_at AS openAt,
                           coupon.close_at AS closeAt,
                           coupon.status AS status,
                           stock.total_quantity AS totalQuantity,
                           stock.total_quantity - stock.active_count
                               AS remainingQuantity
                      FROM coupons coupon
                      JOIN coupon_stocks stock
                        ON stock.coupon_id = coupon.id
                     WHERE (:status IS NULL OR coupon.status = :status)
                     ORDER BY coupon.open_at DESC, coupon.id DESC
                    """,
            countQuery = """
                    SELECT COUNT(*)
                      FROM coupons coupon
                      JOIN coupon_stocks stock
                        ON stock.coupon_id = coupon.id
                     WHERE (:status IS NULL OR coupon.status = :status)
                    """,
            nativeQuery = true
    )
    Page<CouponRoundDetailProjection> findPublicCouponRounds(
            @Param("status") String status,
            Pageable pageable
    );

    @Query(
            value = """
                    SELECT coupon.id AS couponRoundId,
                           coupon.brand_id AS brandId,
                           coupon.name AS name,
                           coupon.policy_type AS policyType,
                           coupon.discount_rate AS discountRate,
                           coupon.max_discount_amount AS maxDiscountAmount,
                           coupon.discount_amount AS discountAmount,
                           coupon.valid_days AS validDays,
                           coupon.open_at AS openAt,
                           coupon.close_at AS closeAt,
                           stock.total_quantity - stock.active_count
                               AS remainingQuantity
                    FROM coupons coupon
                    JOIN coupon_stocks stock
                      ON stock.coupon_id = coupon.id
                    WHERE coupon.status = 'OPEN'
                      AND coupon.open_at <= :asOf
                      AND coupon.close_at > :asOf
                      AND (coupon.eligible_grades_mask
                           & :membershipGradeBit) <> 0
                      AND stock.active_count < stock.total_quantity
                      AND NOT EXISTS (
                            SELECT 1
                            FROM issuances issuance
                            WHERE issuance.coupon_id = coupon.id
                              AND issuance.member_id = :memberId
                      )
                    ORDER BY coupon.close_at ASC, coupon.id ASC
                    """,
            countQuery = """
                    SELECT COUNT(*)
                    FROM coupons coupon
                    JOIN coupon_stocks stock
                      ON stock.coupon_id = coupon.id
                    WHERE coupon.status = 'OPEN'
                      AND coupon.open_at <= :asOf
                      AND coupon.close_at > :asOf
                      AND (coupon.eligible_grades_mask
                           & :membershipGradeBit) <> 0
                      AND stock.active_count < stock.total_quantity
                      AND NOT EXISTS (
                            SELECT 1
                            FROM issuances issuance
                            WHERE issuance.coupon_id = coupon.id
                              AND issuance.member_id = :memberId
                      )
                    """,
            nativeQuery = true
    )
    Page<IssuableCouponRoundProjection> findIssuableCouponRounds(
            @Param("memberId") Long memberId,
            @Param("membershipGradeBit") int membershipGradeBit,
            @Param("asOf") Instant asOf,
            Pageable pageable
    );

    /**
     * V2 정의 목록. <b>결과가 요청 시각에 종속되지 않는 것이 계약이다.</b>
     *
     * <p>예전에는 {@code close_at > :asOf} 로 한 번 더 좁혔는데, 그 결과가 회원 축 없는 단일
     * 캐시 키({@code ALL}) 하나에 담긴다. 시점으로 좁힌 값을 시점 없는 키에 넣으면 <b>누가 먼저
     * 캐시를 채웠는지가 답을 바꾼다</b> — 늦은 요청이 먼저 채우면, 그 사이 닫힌 회차가 더 이른
     * 요청의 목록에서 통째로 사라진다. 인스턴스 시계가 어긋나면 L2 를 건너 같은 일이 벌어진다.
     *
     * <p>지금은 닫히지 않은 회차 전부를 그대로 담고, 시각 판정은 응답 직전 한 곳에서만 한다.
     * 대신 이 집합의 크기는 batch 가 {@code CLOSED} 로 넘기는 속도에 매인다.
     */
    @Query(value = """
            SELECT coupon.id AS couponRoundId,
                   coupon.brand_id AS brandId,
                   coupon.name AS name,
                   coupon.policy_type AS policyType,
                   coupon.discount_rate AS discountRate,
                   coupon.max_discount_amount AS maxDiscountAmount,
                   coupon.discount_amount AS discountAmount,
                   coupon.valid_days AS validDays,
                   coupon.open_at AS openAt,
                   coupon.close_at AS closeAt,
                   coupon.status AS status
              FROM coupons coupon
             WHERE coupon.issuance_engine_version = 'V2'
               AND coupon.status IN ('SCHEDULED', 'OPEN')
             ORDER BY coupon.open_at ASC, coupon.id ASC
            """, nativeQuery = true)
    // 단위는 밀리초다. org.hibernate.timeout 은 초 단위라 최소값이 1초였고, 호출자가 100ms 에
    // 물러난 뒤에도 로더 스레드와 Hikari 커넥션이 최대 1초 더 붙잡혀 발급 경로의 커넥션을
    // 잠식했다(인스턴스 풀은 3이다). 호출자 예산보다 크되 같은 자릿수로 둔다.
    @QueryHints(@QueryHint(name = "jakarta.persistence.query.timeout", value = "300"))
    List<CouponDefinitionProjection> findV2CouponDefinitions();

    @Query("""
            select count(roundEntity)
            from CouponRoundEntity roundEntity
            where roundEntity.openAt < :closeAt
              and roundEntity.closeAt > :openAt
              and roundEntity.status in :statuses
            """)
    long countOverlappingSchedule(
            @Param("openAt") Instant openAt,
            @Param("closeAt") Instant closeAt,
            @Param("statuses") Set<CouponRoundStatus> statuses
    );

    @Query("""
            select roundEntity.id
              from CouponRoundEntity roundEntity
             where roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.OPEN
               and roundEntity.closeAt <= :asOf
             order by roundEntity.id asc
            """)
    List<Long> findClosableOpenRoundIds(@Param("asOf") Instant asOf);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update CouponRoundEntity roundEntity
               set roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.CLOSED
             where roundEntity.id in :roundIds
               and roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.OPEN
               and roundEntity.closeAt <= :asOf
            """)
    int closeOpenRoundsByIds(
            @Param("roundIds") List<Long> roundIds,
            @Param("asOf") Instant asOf
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update CouponRoundEntity roundEntity
               set roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.CLOSED
             where roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.SCHEDULED
               and roundEntity.closeAt <= :asOf
            """)
    int closeMissedScheduledRounds(@Param("asOf") Instant asOf);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE coupons
               SET status = 'OPEN'
             WHERE id = (
                   SELECT candidate.id
                     FROM (
                           SELECT scheduledRound.id
                             FROM coupons scheduledRound
                            WHERE scheduledRound.status = 'SCHEDULED'
                              AND scheduledRound.open_at <= :asOf
                              AND scheduledRound.close_at > :asOf
                              AND NOT EXISTS (
                                    SELECT 1
                                      FROM coupons openRound
                                     WHERE openRound.status = 'OPEN'
                                       AND openRound.open_at < scheduledRound.close_at
                                       AND openRound.close_at > scheduledRound.open_at
                              )
                            ORDER BY scheduledRound.open_at ASC,
                                     scheduledRound.id ASC
                            LIMIT 1
                     ) candidate
             )
            """, nativeQuery = true)
    int openNextScheduledRound(@Param("asOf") Instant asOf);
}
