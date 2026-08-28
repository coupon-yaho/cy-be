package com.kafkick.batch.analytics;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 브랜드 분석 집계(OBS-51)의 설정.
 *
 * @param zone         발급일·시간대를 버킷팅하는 시간대. api 의 {@code ANALYTICS_ZONE} 과 같아야 한다
 * @param watermarkLag 마지막 성공 as_of 에서 되돌려 다시 훑는 폭
 * @param queryTimeout 집계 조회 한 문장의 상한. 관측 풀 기본 상한(3초)을 이 배치만 늘린다
 * @param maxWindowRows 한 걸음이 훑을 이력 행 수의 상한. 밀린 구간을 이 크기로 나눠 따라잡는다
 * @param maxStepsPerRun 한 회차가 밟을 걸음 수의 상한
 */
@ConfigurationProperties(prefix = "batch.analytics")
public record AnalyticsAggregationProperties(
        String zone,
        Duration watermarkLag,
        Duration queryTimeout,
        int maxWindowRows,
        int maxStepsPerRun
) {

    /**
     * 오프셋이 바뀌지 않는지 확인하는 기준점.
     *
     * <p>{@code ZoneRules.isFixedOffset()} 을 쓰지 않는다 — 그건 <b>역사 전체</b>를 보고, 한국은
     * 1948~51 · 1987~88 에 서머타임이 있었으므로 {@code Asia/Seoul} 이 그 검사에서 떨어진다(실측).
     * 여기서 물어야 할 것은 "우리가 버킷팅하는 구간에서 오프셋이 하나인가" 다.
     */
    private static final Instant OFFSET_ANCHOR = Instant.parse("2020-01-01T00:00:00Z");

    public AnalyticsAggregationProperties {
        // 나머지 필드는 전부 사유가 담긴 IAE 인데 여기만 ZoneId.of 의 이름 없는 NPE 였다.
        if (zone == null || zone.isBlank()) {
            throw new IllegalArgumentException("batch.analytics.zone 이 비어 있다.");
        }
        ZoneId zoneId = ZoneId.of(zone);
        // ⚠️ 이 표들은 이 시간대로 **미리** 버킷팅해 저장한다. 앞으로 오프셋이 바뀌는 시간대를 넣으면
        //    같은 issue_date 에 두 오프셋의 발급이 섞이고, 그건 다시 버킷팅할 수 없다 —
        //    조용히 어긋나느니 기동에서 죽인다.
        //
        // ⚠️ 반대 방향 실패 — tzdb 가 갱신돼 이 시간대에 앞으로의 전환이 생기면, 그 순간부터
        //    <b>기동이 막힌다.</b> 값이 조용히 어긋나는 것보다 낫다고 보고 이쪽을 골랐다.
        if (zoneId.getRules().nextTransition(OFFSET_ANCHOR) != null) {
            throw new IllegalArgumentException(
                    "batch.analytics.zone 은 앞으로 오프셋이 바뀌지 않는 시간대여야 한다: " + zone);
        }
        requirePositive(watermarkLag, "watermark-lag");
        requirePositive(queryTimeout, "query-timeout");
        if (maxWindowRows <= 0) {
            throw new IllegalArgumentException("batch.analytics.max-window-rows 는 양수여야 한다.");
        }
        if (maxStepsPerRun <= 0) {
            throw new IllegalArgumentException("batch.analytics.max-steps-per-run 는 양수여야 한다.");
        }
    }

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException("batch.analytics." + name + " 는 양수여야 한다.");
        }
    }

    public ZoneId zoneId() {
        return ZoneId.of(zone);
    }

    /**
     * {@code CONVERT_TZ} 에 넘길 오프셋 문자열({@code +09:00}).
     *
     * <p>이름({@code 'Asia/Seoul'})을 넘기지 않는다 — 그러면 MySQL 의 시간대 표
     * ({@code mysql.time_zone_name})가 적재돼 있어야 하고, 안 적재된 서버에서
     * {@code CONVERT_TZ} 는 <b>예외가 아니라 NULL</b> 을 돌려준다. 그 NULL 은 GROUP BY 키로 들어가
     * 날짜가 통째로 비는 집계를 만든다. 오프셋 형태는 표 없이 동작한다.
     */
    public String zoneOffsetLiteral() {
        ZoneOffset offset = zoneId().getRules().getOffset(OFFSET_ANCHOR);
        return offset.getId().equals("Z") ? "+00:00" : offset.getId();
    }
}
