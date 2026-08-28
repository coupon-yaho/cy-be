package com.kafkick.storage.db.admin;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataReader;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.admin.campaignsource.DetailAvailability;
import com.kafkick.core.admin.campaignsource.PreparationSource;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.SourceStatus;

/** 관측 전용 JDBC 풀에서 관리자 캠페인 카탈로그와 상세 원천값을 조회합니다. */
@Repository
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class JdbcAdminCampaignDataReader implements AdminCampaignDataReader {

    private static final Duration MAX_COUPON_ROUND_DURATION = Duration.ofHours(24);

    private static final Logger log = LoggerFactory.getLogger(JdbcAdminCampaignDataReader.class);

    private static final String CATALOG_SQL = """
            SELECT c.id, c.name, b.name AS brand_name, c.status,
                   c.open_at, c.close_at,
                   c.policy_type, c.discount_rate, c.max_discount_amount, c.discount_amount,
                   c.valid_days, c.eligible_grades_mask,
                   s.total_quantity, s.active_count, s.updated_at
              FROM coupons c
              LEFT JOIN brands b ON b.id = c.brand_id
              LEFT JOIN coupon_stocks s ON s.coupon_id = c.id
             ORDER BY c.open_at DESC, c.id DESC
            """;

    private static final String DETAIL_SQL = """
            SELECT c.id, c.name, b.name AS brand_name, c.status,
                   c.open_at, c.close_at,
                   c.policy_type, c.discount_rate, c.max_discount_amount, c.discount_amount,
                   c.valid_days, c.eligible_grades_mask,
                   s.total_quantity, s.active_count, s.updated_at
              FROM coupons c
              LEFT JOIN brands b ON b.id = c.brand_id
              LEFT JOIN coupon_stocks s ON s.coupon_id = c.id
             WHERE c.id = :couponId
            """;

    private static final String HOLDING_SQL = """
            SELECT status, COUNT(*) AS quantity
              FROM issuances
             WHERE coupon_id = :couponId
             GROUP BY status
            """;

    private static final String TRANSITION_SQL = """
            SELECT h.event_type, COUNT(*) AS quantity
              FROM issuance_histories h
              JOIN issuances i ON i.id = h.issuance_id
             WHERE i.coupon_id = :couponId
               AND h.created_at >= :fromInclusive
               AND h.created_at < :toExclusive
               AND h.event_type IN ('USE', 'CANCEL_USE', 'CANCEL', 'EXPIRE')
             GROUP BY h.event_type
            """;

    private static final RowMapper<CampaignRow> CAMPAIGN_ROW_MAPPER =
            JdbcAdminCampaignDataReader::mapCampaign;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    /** 운영 쓰기 풀과 섞이지 않도록 관측 한정자의 JDBC 템플릿만 주입받습니다. */
    public JdbcAdminCampaignDataReader(
            @Qualifier("obs") NamedParameterJdbcTemplate jdbcTemplate
    ) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate");
    }

    /**
     * 캠페인 전 행과 독립 재고 관측을 지정된 정렬로 조회합니다.
     *
     * <p>브랜드와 재고를 {@code LEFT JOIN}하는 이유는 역정규화 이상을 숨기지 않고, 재고 행이
     * 없는 캠페인도 모집단에 남기기 위해서입니다.</p>
     */
    @Override
    public AdminCampaignCatalog loadCatalog(Instant snapshotAt) {
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        try {
            List<CampaignRow> rows = jdbcTemplate.query(
                    CATALOG_SQL, Map.of(), CAMPAIGN_ROW_MAPPER);
            List<AdminCampaignCatalog.CampaignData> campaigns = new ArrayList<>(rows.size());
            for (CampaignRow row : rows) {
                if (row.brandName() == null) {
                    return unavailableCatalog();
                }
                campaigns.add(new AdminCampaignCatalog.CampaignData(
                        row.couponId(),
                        row.campaignName(),
                        row.brandName(),
                        parseCampaignStatus(row.status()),
                        row.opensAt(),
                        row.closesAt(),
                        stockObservation(row),
                        preparationSource(row, snapshotAt)
                ));
            }
            return new AdminCampaignCatalog(SourceStatus.VALID, snapshotAt, campaigns);
        } catch (RuntimeException exception) {
            log.warn("admin campaign catalog observation failed: snapshotAt={}, exceptionType={}",
                    snapshotAt, exception.getClass().getSimpleName(), exception);
            return unavailableCatalog();
        }
    }

    /**
     * 메타·재고, 현재 보유량과 요청 구간의 상태 전이를 같은 관측 트랜잭션에서 조회합니다.
     *
     * <p>부분 SELECT가 실패하거나 같은 스냅샷의 재고 활성 수와 보유 상태 합계가 다르면 전체
     * 상세를 {@code UNAVAILABLE}로 반환해 불완전한 조합이 계산기로 넘어가지 않게 합니다.</p>
     */
    @Override
    @Transactional(transactionManager = "observationTransactionManager", readOnly = true)
    public AdminCampaignDetailData findDetail(
            long couponId,
            Instant fromInclusive,
            Instant toExclusive,
            Instant snapshotAt
    ) {
        requireDetailArguments(couponId, fromInclusive, toExclusive, snapshotAt);
        try {
            MapSqlParameterSource parameters = new MapSqlParameterSource("couponId", couponId)
                    .addValue("fromInclusive", Timestamp.from(fromInclusive))
                    .addValue("toExclusive", Timestamp.from(toExclusive));
            List<CampaignRow> metadata = jdbcTemplate.query(
                    DETAIL_SQL, parameters, CAMPAIGN_ROW_MAPPER);
            if (metadata.isEmpty()) {
                return new AdminCampaignDetailData(DetailAvailability.NOT_FOUND, null);
            }

            CampaignRow campaign = metadata.getFirst();
            if (campaign.brandName() == null) {
                return unavailableDetail();
            }
            CouponRoundStatus campaignStatus = parseCampaignStatus(campaign.status());
            HoldingResult holding = loadHolding(parameters);
            CouponMetricsSource.Observation<List<CouponMetricsSource.TransitionBucket>> transitions =
                    loadTransitions(parameters, fromInclusive, toExclusive, snapshotAt);
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock =
                    stockObservation(campaign);

            if (stock.status().carriesValue()
                    && stock.value().activeCount()
                    != holding.counts().issued() + holding.counts().used()) {
                log.warn("admin campaign stock drift: couponId={}, activeCount={}, issuedPlusUsed={}",
                        couponId,
                        stock.value().activeCount(),
                        holding.counts().issued() + holding.counts().used());
                return unavailableDetail();
            }

            AdminCampaignDetailData.DetailValue value = new AdminCampaignDetailData.DetailValue(
                    campaign.couponId(),
                    campaign.campaignName(),
                    campaign.brandName(),
                    new CouponMetricsSource.CampaignRuntime(campaignStatus, campaign.opensAt()),
                    stock,
                    new CouponMetricsSource.Observation<>(
                            holding.counts(), holding.status(), snapshotAt),
                    transitions
            );
            return new AdminCampaignDetailData(DetailAvailability.AVAILABLE, value);
        } catch (RuntimeException exception) {
            log.warn("admin campaign detail observation failed: couponId={}, exceptionType={}",
                    couponId, exception.getClass().getSimpleName(), exception);
            return unavailableDetail();
        }
    }

    /** 발급 현재 상태 문자열을 네 상태에 빠짐없이 대응시키고 모르는 값은 즉시 거부합니다. */
    private HoldingResult loadHolding(MapSqlParameterSource parameters) {
        EnumMap<HoldingStatus, Long> quantities = new EnumMap<>(HoldingStatus.class);
        for (HoldingStatus status : HoldingStatus.values()) {
            quantities.put(status, 0L);
        }
        List<StatusCountRow> rows = jdbcTemplate.query(
                HOLDING_SQL,
                parameters,
                (resultSet, rowNumber) -> new StatusCountRow(
                        resultSet.getString("status"), resultSet.getLong("quantity"))
        );
        for (StatusCountRow row : rows) {
            quantities.put(HoldingStatus.valueOf(row.status()), row.quantity());
        }
        CouponMetricsSource.IssuanceStatusCounts counts =
                new CouponMetricsSource.IssuanceStatusCounts(
                        quantities.get(HoldingStatus.ISSUED),
                        quantities.get(HoldingStatus.USED),
                        quantities.get(HoldingStatus.CANCELLED),
                        quantities.get(HoldingStatus.EXPIRED)
                );
        SourceStatus status = rows.isEmpty() ? SourceStatus.NO_TRAFFIC : SourceStatus.VALID;
        return new HoldingResult(counts, status);
    }

    /**
     * 요청 구간 {@code [fromInclusive, toExclusive)}의 네 전이만 하나의 버킷으로 합칩니다.
     * 종료를 제외해야 인접한 다음 구간과 같은 이력을 중복 집계하지 않습니다.
     */
    private CouponMetricsSource.Observation<List<CouponMetricsSource.TransitionBucket>> loadTransitions(
            MapSqlParameterSource parameters,
            Instant fromInclusive,
            Instant toExclusive,
            Instant snapshotAt
    ) {
        EnumMap<TransitionEvent, Long> quantities = new EnumMap<>(TransitionEvent.class);
        for (TransitionEvent event : TransitionEvent.values()) {
            quantities.put(event, 0L);
        }
        List<StatusCountRow> rows = jdbcTemplate.query(
                TRANSITION_SQL,
                parameters,
                (resultSet, rowNumber) -> new StatusCountRow(
                        resultSet.getString("event_type"), resultSet.getLong("quantity"))
        );
        for (StatusCountRow row : rows) {
            quantities.put(TransitionEvent.valueOf(row.status()), row.quantity());
        }
        CouponMetricsSource.TransitionBucket bucket = new CouponMetricsSource.TransitionBucket(
                fromInclusive,
                toExclusive,
                quantities.get(TransitionEvent.USE),
                quantities.get(TransitionEvent.CANCEL_USE),
                quantities.get(TransitionEvent.CANCEL),
                quantities.get(TransitionEvent.EXPIRE)
        );
        SourceStatus status = rows.isEmpty() ? SourceStatus.NO_TRAFFIC : SourceStatus.VALID;
        return new CouponMetricsSource.Observation<>(List.of(bucket), status, snapshotAt);
    }

    /** 재고 LEFT JOIN 부재와 부분 행은 실제 0이 아니므로 값 없는 UNAVAILABLE 관측으로 보존합니다. */
    private static CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stockObservation(
            CampaignRow row
    ) {
        boolean noStock = row.totalQuantity() == null
                && row.activeCount() == null
                && row.stockUpdatedAt() == null;
        if (noStock) {
            return new CouponMetricsSource.Observation<>(null, SourceStatus.UNAVAILABLE, null);
        }
        if (row.totalQuantity() == null
                || row.activeCount() == null
                || row.stockUpdatedAt() == null
                || row.totalQuantity() <= 0L
                || row.activeCount() < 0L
                || row.activeCount() > row.totalQuantity()) {
            // 부분 null과 범위 위반은 0 재고가 아닌 별도 준비 실패로만 사용합니다.
            return new CouponMetricsSource.Observation<>(null, SourceStatus.UNAVAILABLE, null);
        }
        return new CouponMetricsSource.Observation<>(
                new CouponMetricsSource.StockCounts(row.totalQuantity(), row.activeCount()),
                SourceStatus.VALID,
                row.stockUpdatedAt()
        );
    }

    /**
     * 캠페인 설정과 재고 행의 실제 DB 값을 카탈로그 시점의 준비 원천으로 변환합니다.
     *
     * <p>LEFT JOIN으로 재고 행이 없거나 부분 null인 행은 DB 재고 준비 미완료로 보존합니다.
     * 엔진 설정과 실제 발급 경로는 이 Reader가 아닌 Core 계산기가 결합합니다.</p>
     */
    private static PreparationSource preparationSource(CampaignRow row, Instant snapshotAt) {
        boolean databaseStockReady = row.totalQuantity() != null
                && row.totalQuantity() > 0L
                && row.activeCount() != null
                && row.activeCount() >= 0L
                && row.activeCount() <= row.totalQuantity()
                && row.stockUpdatedAt() != null;
        CouponPolicyType policyType = policyTypeOf(row.policyType());
        boolean campaignConfigurationReady = hasValidCampaignConfiguration(row, policyType);
        // 재고 행 부재와 정책 스냅샷 위반은 각각 확정된 DB 준비 실패로 보존합니다.
        return new PreparationSource(
                campaignConfigurationReady, databaseStockReady, policyType, SourceStatus.VALID, snapshotAt);
    }

    /** DB에 저장된 캠페인 스냅샷이 현재 발급 계약의 모든 필수 값을 갖췄는지 확인합니다. */
    private static boolean hasValidCampaignConfiguration(
            CampaignRow row,
            CouponPolicyType policyType
    ) {
        // 실제 회차 도메인이 복원할 수 있는 24시간 이하의 기간만 준비된 설정입니다.
        if (row.campaignName() == null || row.campaignName().isBlank()
                || row.opensAt() == null || row.closesAt() == null
                || !row.opensAt().isBefore(row.closesAt())
                || Duration.between(row.opensAt(), row.closesAt()).compareTo(MAX_COUPON_ROUND_DURATION) > 0
                || row.validDays() == null || row.validDays() <= 0
                || !hasValidGradeMask(row.eligibleGradesMask())) {
            return false;
        }
        return hasValidDiscountPolicy(row, policyType);
    }

    /** 멤버십 enum이 지원하는 비어 있지 않은 등급 비트만 설정값으로 인정합니다. */
    private static boolean hasValidGradeMask(Integer eligibleGradesMask) {
        if (eligibleGradesMask == null) {
            return false;
        }
        try {
            MembershipGrade.fromMask(eligibleGradesMask);
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    /** 정책 종류별 할인 스냅샷 조합이 실제 발급 도메인 규칙과 맞는지 확인합니다. */
    private static boolean hasValidDiscountPolicy(CampaignRow row, CouponPolicyType policyType) {
        if (policyType == null) {
            return false;
        }
        // 각 정책은 자기 전용 혜택 필드만 값이 있어야 준비된 설정입니다.
        return switch (policyType) {
            case PERCENT_CAPPED -> row.discountRate() != null
                    && row.discountRate() >= 1
                    && row.discountRate() <= 100
                    && row.maxDiscountAmount() != null
                    && row.maxDiscountAmount() > 0
                    && row.discountAmount() == null;
            case FIXED_AMOUNT -> row.discountAmount() != null
                    && row.discountAmount() > 0
                    && row.discountRate() == null
                    && row.maxDiscountAmount() == null;
        };
    }

    /** DB 정책 문자열을 관리자 준비 판정 enum으로 변환하며 미지원 값은 설정 실패로 보존합니다. */
    private static CouponPolicyType policyTypeOf(String policyType) {
        if (policyType == null) {
            return null;
        }
        try {
            return CouponPolicyType.valueOf(policyType);
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private static CampaignRow mapCampaign(ResultSet resultSet, int rowNumber) throws SQLException {
        return new CampaignRow(
                resultSet.getLong("id"),
                resultSet.getString("name"),
                resultSet.getString("brand_name"),
                resultSet.getString("status"),
                instant(resultSet, "open_at"),
                instant(resultSet, "close_at"),
                resultSet.getString("policy_type"),
                resultSet.getObject("discount_rate", Integer.class),
                resultSet.getObject("max_discount_amount", Integer.class),
                resultSet.getObject("discount_amount", Integer.class),
                resultSet.getObject("valid_days", Integer.class),
                resultSet.getObject("eligible_grades_mask", Integer.class),
                resultSet.getObject("total_quantity", Long.class),
                resultSet.getObject("active_count", Long.class),
                nullableInstant(resultSet, "updated_at")
        );
    }

    private static Instant instant(ResultSet resultSet, String column) throws SQLException {
        Timestamp value = resultSet.getTimestamp(column);
        if (value == null) {
            throw new IllegalArgumentException(column + "은 null일 수 없습니다.");
        }
        return value.toInstant();
    }

    private static Instant nullableInstant(ResultSet resultSet, String column) throws SQLException {
        Timestamp value = resultSet.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private static CouponRoundStatus parseCampaignStatus(String status) {
        return CouponRoundStatus.valueOf(status);
    }

    private static void requireDetailArguments(
            long couponId,
            Instant fromInclusive,
            Instant toExclusive,
            Instant snapshotAt
    ) {
        if (couponId <= 0L) {
            throw new IllegalArgumentException("couponId는 양수여야 합니다.");
        }
        Objects.requireNonNull(fromInclusive, "fromInclusive");
        Objects.requireNonNull(toExclusive, "toExclusive");
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        if (!toExclusive.isAfter(fromInclusive)) {
            throw new IllegalArgumentException("전이 조회 구간은 양수여야 합니다.");
        }
    }

    private static AdminCampaignCatalog unavailableCatalog() {
        return new AdminCampaignCatalog(SourceStatus.UNAVAILABLE, null, List.of());
    }

    private static AdminCampaignDetailData unavailableDetail() {
        return new AdminCampaignDetailData(DetailAvailability.UNAVAILABLE, null);
    }

    private record CampaignRow(
            long couponId,
            String campaignName,
            String brandName,
            String status,
            Instant opensAt,
            Instant closesAt,
            String policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount,
            Integer validDays,
            Integer eligibleGradesMask,
            Long totalQuantity,
            Long activeCount,
            Instant stockUpdatedAt
    ) { }

    private record StatusCountRow(String status, long quantity) { }

    private record HoldingResult(
            CouponMetricsSource.IssuanceStatusCounts counts,
            SourceStatus status
    ) { }

    private enum HoldingStatus {
        ISSUED,
        USED,
        CANCELLED,
        EXPIRED
    }

    private enum TransitionEvent {
        USE,
        CANCEL_USE,
        CANCEL,
        EXPIRE
    }
}
