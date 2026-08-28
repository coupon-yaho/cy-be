package com.kafkick.batch.analytics;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.coupon.domain.IssuanceEventType;

/**
 * 집계 원천을 <b>관측 풀</b>로 읽는다. 이 클래스가 관측 풀을 무는 유일한 자리다.
 *
 * <p><b>왜 관측 풀인가.</b> 재계수는 {@code issuances} 를 회차 단위로 훑는 무거운 조회다. 운영 풀로
 * 나가면 하필 그 풀이 v1 의 병목이라 회차 간 처리량 비교가 오염된다 —
 * {@code ConsistencyRawValueReader} 가 관측 풀로 나간 것과 같은 이유다.
 *
 * <p>여기서 읽는 세 표({@code issuances} · {@code issuance_histories} · {@code coupons})는 모두
 * {@code infra/mysql/obs-grants/allowlist.txt} 에 이미 있다. 목록 밖 표를 읽으면
 * {@code ObservationAccountPrivilegeTest} 가 잡는다.
 *
 * <p>⚠️ 쓰기는 여기 없다. 관측 계정은 SELECT 만 갖는다 — 회차 이력과 집계 행은
 * {@link AnalyticsRunStore} 가 운영 풀로 쓴다.
 */
public class AnalyticsAggregateReader {

    /**
     * 상태 네 개를 <b>손으로 적는다.</b> {@code IssuanceStatus} 전체를 따라가면 새 상태가 생겼을 때
     * 그 행까지 세어 조용히 맞는 합계를 만든다. 여기 안 적힌 상태가 생기면 {@code COUNT(*)} 와 네
     * 합계가 갈라져 {@code ck_analytics_status_total} 이 그 자리에서 막는다 — 그 실패가
     * "새 상태가 발급 이력인지는 사람이 정해야 한다" 는 신호다.
     */
    private static final String STATUS_COLUMNS = """
            SUM(i.status = 'ISSUED') AS currently_issued,
            SUM(i.status = 'USED') AS used,
            SUM(i.status = 'CANCELLED') AS cancelled,
            SUM(i.status = 'EXPIRED') AS expired""";

    /**
     * 마지막 성공 이후 바뀐 (발급일, 회차) 버킷.
     *
     * <p>발급·사용·취소·만료가 <b>전부</b> 이력 한 행을 남긴다(발급도 {@code ISSUE} 이벤트를 남긴다 —
     * {@code IssuanceHistory.issue()}). 그래서 {@code idx_issuance_histories_created_id} 범위 조회
     * 하나가 "무엇이 바뀌었나" 를 전부 답한다. {@code issuances} 에는 인덱스를 추가하지 않는다 —
     * 발급 경로의 쓰기 테이블이다.
     *
     * <p>{@code created_at <= as_of} 상한도 함께 건다. 상한이 없으면 같은 {@code as_of} 재실행에서
     * 대상 버킷이 달라진다.
     */
    private static final String CHANGED_BUCKETS = """
            SELECT DISTINCT
                   DATE(CONVERT_TZ(i.issued_at, '+00:00', ?)) AS issue_date,
                   i.coupon_id AS coupon_id
            FROM issuance_histories h
            JOIN issuances i ON i.id = h.issuance_id
            WHERE h.created_at > ? AND h.created_at <= ?""";

    /**
     * 발급 수 두 축은 {@code ISSUE} 이벤트만 본다.
     *
     * <p>그 두 축은 {@code issued_at} 으로 세므로 <b>발급이 새로 생길 때만</b> 값이 변한다.
     * 사용·취소·만료는 같은 발급의 상태를 바꿀 뿐 발급 수를 바꾸지 않는다.
     *
     * <p>⚠️ 이 필터가 없으면 <b>만료 배치 직후가 위험하다</b> — 만료 일괄 처리가 이력 수만 행을
     * 남기고, 그 때문에 값이 바뀌지도 않을 과거 버킷들이 통째로 재계수 대상이 된다.
     * 재계수 한 건이 회차 전량 스캔이라 그대로 상한을 넘긴다.
     *
     * <p>상태 축에는 걸지 않는다. 거기는 정확히 그 상태 전이를 세는 축이다.
     *
     * <p>값을 손으로 적지 않고 {@link IssuanceEventType#ISSUE} 에서 가져온다. 문자열로 옮겨 적으면
     * 그 이름이 바뀌었을 때 <b>필터가 아무것도 맞추지 못해</b> 발급 수 두 축이 조용히 멈춘다 —
     * 예외도 로그도 없고 화면만 갱신이 끊긴다.
     */
    private static final String ISSUE_EVENTS_ONLY =
            "\n  AND h.event_type = '" + IssuanceEventType.ISSUE.name() + "'";

    /**
     * 버킷 하루를 UTC 범위로 편다. {@code DATE(i.issued_at)} 로 비교하면 행마다 계산이라 인덱스가
     * 아예 후보에서 빠진다.
     *
     * <p>{@code i.issued_at <= 끝점} 이 <b>재실행 결정성의 전부</b>다. 이것이 없으면 같은
     * 기준으로 다시 돌릴 때 그 사이 들어온 발급까지 세어 값이 달라진다.
     *
     * <p>⚠️ 이 상한은 <b>발견 창의 상한과 같아야 한다.</b> 발견은 {@code h.created_at <= 끝점} 이고
     * ISSUE 이력은 {@code created_at = issued_at} 이라, 여기만 {@code <} 로 두면 끝점에 정확히 걸친
     * 발급이 <b>발견되고도 안 세어진다.</b> 그 버킷은 다음 걸음의 발견 창에도 안 들어와 영영
     * 한 건 모자란 채로 남는다(실측 — 걸음 창을 맞닿게 고치자 이 자리가 드러났다. 그전에는
     * 겹쳐 훑기가 우연히 덮고 있었다).
     */
    private static final String BUCKET_DAY_RANGE = """
            AND i.issued_at >= CONVERT_TZ(CAST(b.issue_date AS DATETIME), ?, '+00:00')
            AND i.issued_at <  CONVERT_TZ(CAST(b.issue_date AS DATETIME) + INTERVAL 1 DAY, ?, '+00:00')
            AND i.issued_at <= ?""";

    private final JdbcTemplate observationJdbcTemplate;
    private final AnalyticsAggregationProperties properties;

    /**
     * 관측 <b>풀</b>을 받아 이 배치 전용 {@code JdbcTemplate} 을 만든다.
     *
     * <p>공용 {@code @Qualifier("obs") JdbcTemplate} 을 그대로 쓰지 않는 이유 — 그 빈은
     * {@code observation.datasource.query-timeout: 3s} 를 {@code setQueryTimeout} 으로 걸고 있다.
     * 1초 폴링 수집에 맞춘 값이라 이 집계는 거기서 잘린다. <b>SQL 힌트로는 이 겹이 안 풀린다</b> —
     * 힌트는 서버 쪽 {@code max_execution_time} 만 덮고, JDBC 쪽 취소는 드라이버가 건다.
     *
     * <p>⚠️ 그래도 <b>세 번째 겹</b>이 남는다: 관측 URL 의 {@code socketTimeout=5000}. 그 값보다
     * 오래 걸리는 집계는 여기서 무엇을 설정해도 커넥션이 끊긴다 — 축이 UNAVAILABLE 로 남고
     * 회차는 FAILED 가 된다(조용히 틀린 값이 아니라 드러나는 실패다).
     * TODO(후속 티켓): 부하 중 이 집계의 소요를 재고, 필요하면 storage.yml 의
     * {@code OBS_SOCKET_TIMEOUT_MS} 를 이 배치가 감당할 값으로 올린다. 그 파일은 이 티켓의
     * 소유가 아니다.
     */
    public AnalyticsAggregateReader(
            @Qualifier("obs") DataSource observationDataSource,
            AnalyticsAggregationProperties properties
    ) {
        this.observationJdbcTemplate = new JdbcTemplate(observationDataSource);
        // JDBC API 가 초 단위다. 내리면 설정보다 빨리 끊기고 0 은 '제한 없음' 이 되므로 올린다
        // (관측 템플릿이 같은 이유로 올림한다).
        this.observationJdbcTemplate.setQueryTimeout(
                (int) Math.ceil(properties.queryTimeout().toMillis() / 1000.0));
        this.properties = properties;
    }

    /** 축 1 — 월별 추이. {@code issued_at} 기준이라 같은 {@code as_of} 재실행이 같은 값을 낸다. */
    public List<DailyRow> readDaily(Instant since, Instant asOf) {
        String sql = hint() + """
                b.issue_date, b.coupon_id, c.brand_id, COUNT(*) AS issue_count
                FROM (
                """ + CHANGED_BUCKETS + ISSUE_EVENTS_ONLY + """
                ) b
                JOIN coupons c ON c.id = b.coupon_id
                JOIN issuances i ON i.coupon_id = b.coupon_id
                """ + BUCKET_DAY_RANGE + """
                GROUP BY b.issue_date, b.coupon_id, c.brand_id""";
        return observationJdbcTemplate.query(sql,
                (rs, rowNum) -> new DailyRow(
                        rs.getObject(1, LocalDate.class), rs.getLong(2), rs.getLong(3), rs.getLong(4)),
                bucketArguments(since, asOf));
    }

    /** 축 2 — 요일·시간대 히트맵. 요일은 저장하지 않는다({@code issue_date} 에서 나오는 값이다). */
    public List<HourlyRow> readHourly(Instant since, Instant asOf) {
        String sql = hint() + """
                b.issue_date, HOUR(CONVERT_TZ(i.issued_at, '+00:00', ?)) AS issue_hour,
                b.coupon_id, c.brand_id, COUNT(*) AS issue_count
                FROM (
                """ + CHANGED_BUCKETS + ISSUE_EVENTS_ONLY + """
                ) b
                JOIN coupons c ON c.id = b.coupon_id
                JOIN issuances i ON i.coupon_id = b.coupon_id
                """ + BUCKET_DAY_RANGE + """
                GROUP BY b.issue_date, issue_hour, b.coupon_id, c.brand_id""";
        List<Object> arguments = new ArrayList<>();
        arguments.add(properties.zoneOffsetLiteral());
        arguments.addAll(List.of(bucketArguments(since, asOf)));
        return observationJdbcTemplate.query(sql,
                (rs, rowNum) -> new HourlyRow(
                        rs.getObject(1, LocalDate.class), rs.getInt(2),
                        rs.getLong(3), rs.getLong(4), rs.getLong(5)),
                arguments.toArray());
    }

    /**
     * 축 3 — 발급일별 현재 상태 분포.
     *
     * <p>⚠️ 이 축만 재실행 결정성이 없다. {@code issuances.status} 가 계속 바뀌기 때문이다 —
     * 만료가 지나도 굳지 않는다({@code CouponStateMachine} 의 {@code CANCEL_USE} 가 만료 뒤에도
     * 허용되고 취소 시한이 core·API·DB 어디에도 없다). 그래서 이 축은 {@code observed_at} 이 답한다.
     */
    public List<StatusRow> readStatuses(Instant since, Instant asOf, Instant observedAt) {
        String sql = hint() + """
                b.issue_date, b.coupon_id, c.brand_id, COUNT(*) AS total_issued,
                """ + STATUS_COLUMNS + """

                FROM (
                """ + CHANGED_BUCKETS + """
                ) b
                JOIN coupons c ON c.id = b.coupon_id
                JOIN issuances i ON i.coupon_id = b.coupon_id
                """ + BUCKET_DAY_RANGE + """
                GROUP BY b.issue_date, b.coupon_id, c.brand_id""";
        return observationJdbcTemplate.query(sql,
                (rs, rowNum) -> new StatusRow(
                        rs.getObject(1, LocalDate.class), rs.getLong(2), rs.getLong(3),
                        rs.getLong(4), rs.getLong(5), rs.getLong(6), rs.getLong(7), rs.getLong(8),
                        observedAt),
                bucketArguments(since, asOf));
    }

    /**
     * 다음 걸음의 <b>끝점</b>. 이력 {@code maxWindowRows} 행이 들어오는 지점까지만 잡는다.
     *
     * <h2>왜 시간이 아니라 행 수인가 — 실측</h2>
     *
     * <p>부하 시험 회차는 300만 건이 <b>30분 안에</b> 몰린다. 그래서 시간으로 창을 잘라도 6시간이든
     * 1시간이든 그 안에 전부 들어온다. 실측(MySQL 8.4 · 컨테이너 · 버퍼풀 1GB · 유휴):
     *
     * <pre>
     *   창      한 걸음   300만 따라잡기   4초 상한 대비
     *   20만    0.75s     15걸음 11.3s     5.3배
     *   50만    1.20s      6걸음  7.2s     3.3배
     *   100만   1.65s      3걸음  5.0s     2.4배   ← 기본값
     *   300만   3.57s      1걸음  3.6s     1.1배   (안 자른 경우 — 여유가 없다)
     * </pre>
     *
     * <p>즉 창을 안 자르면 한 문장이 상한에 붙는다 — 넘으면 축이 AVAILABLE 을 못 받고, 그러면
     * 집계 지점이 안 움직이고, 다음 회차도 같은 창으로 또 죽는다. 스스로 못 빠져나오는 자리다.
     *
     * <p>⚠️ 걸음을 잘게 쪼갤수록 <b>총 작업량은 늘어난다</b> — 재계수는 걸음마다 회차 전량 스캔이라
     * 걸음 수만큼 곱해진다. 그래서 "작을수록 안전" 이 아니라, 상한에 여유가 남는 선에서 가장 크게
     * 잡는 것이 맞다. 그 곱셈을 없애려면 버킷 수집과 재계수를 분리해야 하는데, 그러면 걸음마다
     * 커밋해 진행분을 남기는 성질을 잃는다 — 후속 티켓으로 남겼다.
     *
     * <p>{@code ORDER BY created_at, id} 는 {@code idx_issuance_histories_created_id} 를 그대로 탄다.
     *
     * <p>⚠️ {@code since} 는 <b>그 걸음이 실제로 읽을 창의 하한</b>이다(커서가 아니다). 끝점을
     * 커서부터 세면 실제 창은 lag 구간만큼 더 커져 상한을 넘는다 — 예산을 행 수로 잡았으므로
     * 하한과 끝점은 같은 지점에서 출발해야 한다. 하한이 커서보다 앞이라 끝점이 커서를 못 넘는
     * 경우는 호출부가 하한을 커서로 당겨 다시 부른다.
     *
     * <p>동률 처리 — 창은 {@code created_at <= 끝점} 이고 끝점은 앞 N행의 최대값이라, N 밖에 있는
     * 같은 시각의 행도 함께 들어온다. 다음 걸음은 {@code > 끝점} 이라 겹치지도 빠지지도 않는다.
     *
     * @return 다음 끝점. 창에 이력이 하나도 없으면 {@code asOf}(더 볼 것이 없다는 뜻)
     */
    public Instant nextStepBoundary(Instant since, Instant asOf, int maxWindowRows) {
        LocalDateTime boundary = observationJdbcTemplate.queryForObject(hint() + """
                MAX(created_at) FROM (
                    SELECT created_at FROM issuance_histories
                    WHERE created_at > ? AND created_at <= ?
                    ORDER BY created_at, id LIMIT ?) x""",
                LocalDateTime.class, utc(since), utc(asOf), maxWindowRows);
        return boundary == null ? asOf : boundary.toInstant(ZoneOffset.UTC);
    }

    /**
     * 창의 <b>하한</b>을 행 수로 묶는다. 끝점이 이미 {@code asOf} 로 정해진 걸음에 쓴다.
     *
     * <p>새로 볼 구간이 없고 늦은 커밋 겹쳐 훑기만 남은 걸음은 끝점을 고를 여지가 없다. 그때
     * 하한을 그대로 두면 창 크기가 <b>행 수가 아니라 lag 로만</b> 정해져 예산 밖으로 나간다 —
     * 이 배치의 전제(10분에 100만 행)면 lag 구간 하나가 기본 상한과 맞먹는다.
     *
     * <p>{@code OFFSET} 자리에 행이 있으면 창에 상한보다 많은 이력이 있다는 뜻이고, 그 행의 시각을
     * 새 하한으로 삼는다. 없으면 이미 상한 안이라 하한을 그대로 둔다.
     *
     * <p>⚠️ 자르는 자리는 <b>시각 경계</b>다. 조회 창이 {@code created_at > 하한} 이라 그 시각의
     * 행은 통째로 창 밖에 남는다 — 그것이 의도다. 동률을 포함시키려고 하한을 앞으로 당기면 창이
     * 상한을 넘고, 그러면 이 배치가 스스로 못 빠져나오는 자리(문장이 상한 초과 → 축이 AVAILABLE
     * 을 못 받음 → 수위선 정지)로 돌아간다. 실측으로 정한 예산이 그 상한이다.
     *
     * <p>잘려 나가는 행은 <b>이미 수위선 뒤</b>, 즉 앞선 회차가 덮은 구간이다. 여기서 다시 훑는
     * 것은 늦은 커밋을 줍기 위한 <b>최선 노력</b>이고, 그 최선 노력의 범위를 행 수로 묶는다.
     */
    public Instant boundedWindowStart(Instant floor, Instant asOf, int maxWindowRows) {
        LocalDateTime overflow = observationJdbcTemplate.query(hint() + """
                created_at FROM issuance_histories
                WHERE created_at > ? AND created_at <= ?
                ORDER BY created_at DESC, id DESC LIMIT 1 OFFSET ?""",
                rs -> rs.next() ? rs.getObject(1, LocalDateTime.class) : null,
                utc(floor), utc(asOf), maxWindowRows);
        return overflow == null ? floor : overflow.toInstant(ZoneOffset.UTC);
    }

    /**
     * 관측 풀은 서버 쪽 {@code max_execution_time=3000} 과 3초 JDBC 상한을 갖는다. 1초 폴링 수집에
     * 맞춘 값이라 이 집계는 그 상한에 걸린다 — 문장 힌트로 <b>이 배치의 문장만</b> 늘린다.
     * (힌트는 최상위 SELECT 에서만 유효하다. 파생 표 안에 두면 조용히 무시된다.)
     *
     * <p>⚠️ 반대 방향 실패 — 관측 풀은 커넥션이 2개다. 이 문장이 오래 걸리면 그동안 도메인 Gauge 는
     * 남은 1개로 돈다. 값을 키울수록 그 구간이 길어진다. 그리고 상한을 늘린 만큼, 인덱스가 없어
     * 느려진 집계가 <b>실패 대신 오래 도는</b> 쪽으로 나타난다.
     */
    private String hint() {
        return "SELECT /*+ MAX_EXECUTION_TIME(" + properties.queryTimeout().toMillis() + ") */\n";
    }

    private Object[] bucketArguments(Instant since, Instant asOf) {
        String offset = properties.zoneOffsetLiteral();
        return new Object[] {
                offset, utc(since), utc(asOf),   // 바뀐 버킷 찾기
                offset, offset, utc(asOf)        // 버킷 하루를 UTC 범위로
        };
    }

    /**
     * 컬럼은 UTC 로 저장된다({@code storage.yml} 의 {@code serverTimezone=UTC}). {@code Timestamp} 로
     * 넘기면 드라이버가 JVM 기본 시간대를 거쳐 변환하므로, 변환이 없는 {@code LocalDateTime} 으로 낸다.
     */
    static LocalDateTime utc(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    public record DailyRow(LocalDate date, long couponId, long brandId, long issueCount) {}

    public record HourlyRow(LocalDate date, int hour, long couponId, long brandId, long issueCount) {}

    public record StatusRow(
            LocalDate date, long couponId, long brandId,
            long totalIssued, long currentlyIssued, long used, long cancelled, long expired,
            Instant observedAt) {}
}
