package com.kafkick.batch.observation;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.consistency.ConsistencyRawSnapshot;
import com.kafkick.core.consistency.ConsistencyRawValues;
import com.kafkick.core.consistency.SourceObservation;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/**
 * 정합성 원시값 7종과 대기열·재고·마지막 발급 시각을 읽는다.
 *
 * <p><b>왜 batch 인가</b> — issuances 집계는 300만 행짜리다. 발급 경로에 얹으면 측정 대상이 관측
 * 때문에 느려진다. 그리고 운영 풀이 아니라 관측 전용 풀({@code @Qualifier("obs")})로만 나간다.
 * 운영 풀 10개 중 하나를 상시 점유하면 하필 그 풀이 v1 의 병목이라 회차 비교가 무효가 된다.
 *
 * <p><b>비싼 것과 싼 것을 나눈다.</b> {@link #readStock()} 은 PK 조회 두 건이라 1초마다 돌아도
 * 되고, {@link #read()} 는 issuances 를 훑으므로 훨씬 느린 주기로 돈다. 대신 정합성에 쓰이는
 * 값들은 <b>반드시 같은 문장 안에서</b> 읽는다 — 재고 카운터와 행 집계를 다른 시점에 읽으면
 * 그 사이의 발급이 그대로 가짜 격차가 되어, 없는 정합성 사고를 만들어 낸다.
 *
 * <p>조회에 실패하면 예외를 올리지 않고 해당 원천을 UNAVAILABLE 로 돌려준다. 숫자를 지어내지
 * 않는 것이 이 클래스의 계약이다.
 */
public class ConsistencyRawValueReader {

    private static final Logger log = LoggerFactory.getLogger(ConsistencyRawValueReader.class);

    /** 재고 불변식의 분자. SQL 리터럴을 손으로 적으면 상태가 하나 늘어날 때 여기만 안 바뀐다. */
    private static final String ACTIVE_STATUS_LIST = statusList(IssuanceStatus::countsTowardStock);

    /**
     * 누적 발급의 분자. {@code ConsistencyRawValues} 계약이 네 상태를 <b>열거</b>하므로
     * {@code COUNT(*)} 로 세면 안 된다 — 계약 밖 상태가 생기면 그 행까지 세어 persist gap 이
     * 상시 음수가 된다.
     *
     * <p>enum 전체를 쓰지 않고 <b>네 개를 적어 둔다.</b> 자동으로 따라가면 발급 전 단계 같은 새
     * 상태까지 세어 같은 실패로 돌아간다 — 새 상태가 "발급 이력" 인지는 사람이 정해야 한다.
     * 그 시점은 {@code ConsistencyRawValueReaderTest} 가 알려 준다.
     */
    private static final String ISSUED_EVER_STATUS_LIST = statusList(EnumSet.of(
        IssuanceStatus.ISSUED, IssuanceStatus.USED,
        IssuanceStatus.CANCELLED, IssuanceStatus.EXPIRED)::contains);

    private static String statusList(java.util.function.Predicate<IssuanceStatus> filter) {
        return Arrays.stream(IssuanceStatus.values())
            .filter(filter)
            .map(status -> "'" + status.name() + "'")
            .collect(Collectors.joining(", "));
    }

    /**
     * 관측 대상 회차를 고정하지 않았을 때의 대상. 가장 최근에 열린 회차 하나만 본다.
     *
     * <p>⚠️ 부하 중에 새 회차가 열리면 관측 대상이 그쪽으로 넘어간다. 회차를 고정하려면
     * {@code observation.domain-gauge.coupon-id} 를 지정한다.
     *
     * <p>TODO(OBS-14b 담당): 회차 출처를 {@code benchmark_runs} 의 진행 중인 행
     * ({@code stopped_at IS NULL})으로 바꾼다. 측정 시작(benchmark start)은 batch 가 이미 떠 있는
     * 뒤에 일어나므로, 지금처럼 환경변수로 박으려면 <b>회차마다 batch 를 재시작</b>해야 한다.
     * 그 테이블이 생기는 시점은 {@code BatchBenchmarkRunsAssumptionTest} 가 알려 준다.
     */
    // LEFT JOIN 인 이유 — 스키마는 재고 행 없는 회차를 허용한다(외래키가 coupon_stocks → coupons
    // 한 방향뿐이다). INNER JOIN 이면 그런 회차가 결과에서 통째로 사라져, 관측이 이전 회차를
    // 조용히 계속 보여준다. 잡아 두고 "재고를 모른다" 고 말하는 편이 낫다.
    private static final String LATEST_COUPON_SQL = """
        SELECT c.id AS coupon_id, s.total_quantity, s.active_count
          FROM coupons c
          LEFT JOIN coupon_stocks s ON s.coupon_id = c.id
         WHERE c.open_at <= ?
         ORDER BY c.open_at DESC, c.id DESC
         LIMIT 1
        """;

    private static final String FIXED_COUPON_SQL = """
        SELECT c.id AS coupon_id, s.total_quantity, s.active_count
          FROM coupons c
          LEFT JOIN coupon_stocks s ON s.coupon_id = c.id
         WHERE c.id = ?
        """;

    /**
     * 상관 서브쿼리 세 개 대신 한 번의 스캔으로 끝낸다. 같은 회차 행을 세 번 훑으면 관측이 그
     * 자체로 부하가 되어 측정 대상을 바꾼다.
     *
     * <p><b>실측(MySQL 8.4 · 한 회차 300만 행 · buffer pool 512M)</b> — 세 값을 함께 구하는 이
     * 문장이 약 1.5초다. COUNT 만 남겨도 1.5초라 {@code MAX(issued_at)} 은 사실상 공짜로 얹혀
     * 간다. 반대로 인덱스로는 줄지 않는다 — 한 회차가 300만 행이면 어떤 인덱스를 얹어도 그
     * 회차의 엔트리를 전부 세어야 해서 {@code (coupon_id, status, issued_at)} 을 추가해도
     * 1.5초 그대로였다. 그래서 인덱스를 추가하지 않는다(발급 경로 쓰기 비용만 늘 뿐이다).
     *
     * <p>⚠️ 1.5초는 관측 풀의 서버 상한 {@code max_execution_time=3000} 에 가깝다. 버퍼풀이
     * 차갑거나 부하가 겹치면 잘린다. 잘리면 값이 아니라 UNAVAILABLE 이 나가고, 연속 실패는
     * {@link com.kafkick.core.observation.DomainMeterNames#COLLECT_LAST_SUCCESS_EPOCH} 로 보인다 —
     * 실패가 이어지면 그 시각이 멈춰 있다. 연속 실패 <b>횟수</b>는 미터로 나가지 않고 로그에만 남는다.
     */
    private static final String AGGREGATE_SQL = """
        SELECT s.total_quantity,
               s.active_count,
               COALESCE(a.db_active_count, 0) AS db_active_count,
               COALESCE(a.db_issued_ever_count, 0) AS db_issued_ever_count,
               a.last_issued_at
          FROM coupon_stocks s
          LEFT JOIN (
                SELECT i.coupon_id,
                       SUM(i.status IN (%s)) AS db_active_count,
                       SUM(i.status IN (%s)) AS db_issued_ever_count,
                       MAX(i.issued_at) AS last_issued_at
                  FROM issuances i
                 WHERE i.coupon_id = ?
                 GROUP BY i.coupon_id
               ) a ON a.coupon_id = s.coupon_id
         WHERE s.coupon_id = ?
        """.formatted(ACTIVE_STATUS_LIST, ISSUED_EVER_STATUS_LIST);

    /**
     * 정합성에 쓰이는 Redis 값 셋을 한 번의 왕복으로, 그리고 <b>원자적으로</b> 읽는다.
     *
     * <p>왕복을 나누면 그 사이의 발급이 LUA_GAP 에 그대로 남는다. LUA_GAP 은 크기와 무관하게
     * CRITICAL 이라, 정상 동작 중에 원자성 위반 경보가 뜬다.
     *
     * <p>회원 집합의 자료구조는 V2·V3 구현이 정하므로 여기서 하나로 못 박지 않는다. 다만 지원하지
     * 않는 자료형은 {@link #INVALID_TYPE_MARKER} 로 돌려 예열과 구분한다.
     */
    /** Lua 가 "이 키는 크기를 잴 수 없는 자료형" 을 알리는 표식. 예열(false)과 구분된다. */
    private static final String INVALID_TYPE_MARKER = "INVALID_TYPE";

    private static final RedisScript<List> CONSISTENCY_SCRIPT = new DefaultRedisScript<>("""
        local function size(key)
          local t = redis.call('TYPE', key)['ok']
          if t == 'set' then return redis.call('SCARD', key)
          elseif t == 'zset' then return redis.call('ZCARD', key)
          elseif t == 'list' then return redis.call('LLEN', key)
          elseif t == 'string' then return redis.call('GET', key)
          -- 키가 없는 것은 예열이라 곧 값이 나오지만, 엉뚱한 자료형은 키를 잘못 가리킨 설정이라
          -- 영영 안 나온다. 둘을 같은 false 로 뭉개면 오설정이 예열로 위장한다.
          elseif t == 'none' then return false
          else return 'INVALID_TYPE' end
        end
        return { redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2]), size(KEYS[3]) }
        """, List.class);

    private final JdbcTemplate observationJdbcTemplate;
    private final StringRedisTemplate redisTemplate;
    private final DomainGaugeProperties properties;
    private final TimeProvider timeProvider;

    /**
     * @param observationJdbcTemplate 관측 전용 풀에 붙은 JdbcTemplate
     * @param redisTemplate Redis 조회 통로; V1 이거나 아직 배선되지 않았으면 {@code null}
     * @param properties 엔진 버전·관측 대상 회차·Redis 키 규약
     * @param timeProvider 관측 시각의 유일한 출처. {@code Instant.now()} 를 직접 부르면
     *     {@code Clock.fixed} 를 건 테스트에서 이 리더만 실시간을 봐 회차 선택이 고정되지 않는다
     */
    public ConsistencyRawValueReader(
        @Qualifier("obs") JdbcTemplate observationJdbcTemplate,
        StringRedisTemplate redisTemplate,
        DomainGaugeProperties properties,
        TimeProvider timeProvider
    ) {
        this.observationJdbcTemplate = observationJdbcTemplate;
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.timeProvider = timeProvider;
        if (properties.couponId() == null) {
            // ⚠️ 이 경고가 이 상황을 막는 유일한 장치다. 회차가 넘어가도 예외도 그래프 단절도 없이
            // 숫자만 바뀐다. 측정 전에 회차를 박았는지는 저장소가 확인할 수 없다 — 값이 실행 시점
            // 환경변수로 들어오기 때문이다. 위 TODO(OBS-14b) 가 붙으면 사람이 박을 필요가 없어진다.
            log.warn("관측 대상 회차를 고정하지 않았다. 새 회차가 열리면 지표가 그 회차로 넘어간다."
                + " 회차 간 비교가 필요하면 observation.domain-gauge.coupon-id 를 지정한다.");
        }
    }

    /**
     * 1초 주기로 도는 가벼운 조회. PK 조회라 issuances 를 훑지 않는다.
     *
     * @return 관측 대상 회차와 재고 잔량·대기열 길이
     */
    public StockSnapshot readStock() {
        CouponRow coupon;
        try {
            coupon = queryCoupon();
        } catch (DataAccessException exception) {
            log.warn("관측 DB 재고 조회에 실패했다. 값 대신 UNAVAILABLE 을 내보낸다.", exception);
            return new StockSnapshot(null, null, SourceStatus.UNAVAILABLE,
                null, queueStatusFor(SourceStatus.UNAVAILABLE));
        }
        if (coupon == null) {
            SourceStatus status = missingCouponStatus();
            // 회차가 아직 없는 것과 박은 회차가 없는 것은 다른 사건이다. 0 은 "재고가 다 나갔다" 로
            // 읽히므로 어느 쪽이든 값은 만들지 않는다.
            return new StockSnapshot(null, null, status, null, queueStatusFor(status));
        }
        if (!coupon.hasStock()) {
            // 회차 = 재고 N 장을 여는 것이다. 재고 행이 없는 회차는 미완성 데이터이지 "대기 중" 이
            // 아니다. 어느 회차가 그런지는 값으로 알 수 있게 회차 번호는 그대로 싣는다.
            log.warn("회차 {} 에 재고 행이 없다. 재고를 알 수 없어 값 대신 UNAVAILABLE 을 내보낸다.",
                coupon.couponId());
            return new StockSnapshot(coupon.couponId(), null, SourceStatus.UNAVAILABLE,
                null, queueStatusFor(SourceStatus.UNAVAILABLE));
        }
        if (properties.engineVersion() == EngineVersion.V1) {
            // V1 에는 Redis 도 대기열도 없다. 없는 원천의 값은 0 이 아니라 "해당 없음" 이다.
            return new StockSnapshot(
                coupon.couponId(),
                coupon.totalQuantity() - coupon.storedActiveCount(), SourceStatus.VALID,
                null, SourceStatus.N_A);
        }
        return redisStock(coupon);
    }

    /**
     * 정합성 원시값 7종을 읽는다. issuances 를 훑으므로 {@link #readStock()} 보다 느린 주기로 돈다.
     *
     * @return 원천별 상태가 함께 붙은 스냅샷; 조회 실패는 예외가 아니라 상태로 표현된다
     */
    public DomainRawSnapshot read() {
        AggregateRow row;
        RedisRow redis;
        Instant observedAt;
        CouponRow coupon = null;
        try {
            coupon = queryCoupon();
            if (coupon == null) {
                return missingCouponStatus() == SourceStatus.UNAVAILABLE
                    ? unavailableDatabase(properties.couponId())
                    : pending();
            }
            if (!coupon.hasStock()) {
                // 재고 행이 없으면 DB_COUNTER_GAP 의 한쪽 항이 통째로 없다. 0 으로 채우면 그 자리가
                // 가짜 격차가 된다.
                log.warn("회차 {} 에 재고 행이 없다. 정합성 원시값을 만들 수 없다.", coupon.couponId());
                return unavailableDatabase(coupon.couponId());
            }
            // Redis 를 먼저 읽는다. 두 원천을 같은 순간에 얼릴 방법은 없으므로 순서를 고정해
            // 시차의 방향을 하나로 만든다 — Redis 가 항상 더 과거다. 크기는 미터로 함께 낸다.
            redis = readRedisForConsistency(coupon.couponId());
            observedAt = timeProvider.instant();
            row = queryAggregate(coupon.couponId());
        } catch (DataAccessException exception) {
            log.warn("관측 DB 집계에 실패했다. 값 대신 UNAVAILABLE 을 내보낸다.", exception);
            // 삼항의 두 갈래를 모두 Long 으로 둔다. 한쪽이 long 이면 값이 없는 쪽이 언박싱되어 NPE 다.
            return unavailableDatabase(
                coupon == null ? properties.couponId() : Long.valueOf(coupon.couponId()));
        }
        if (row == null) {
            // hasStock() 을 통과한 뒤 집계가 0행이면 그 사이에 재고 행이 사라진 것이다. 파손이지
            // 대기가 아니다 — PENDING 으로 두면 실패로 세지 않아 경보가 영영 안 걸린다.
            log.warn("회차 {} 의 집계가 0행이다. 재고 행이 사라졌다.", coupon.couponId());
            return unavailableDatabase(coupon.couponId());
        }

        ConsistencyRawValues values = new ConsistencyRawValues(
            row.totalQuantity(),
            redis.remaining(),
            redis.issuedEverCount(),
            redis.memberEverCount(),
            row.dbActiveCount(),
            row.dbIssuedEverCount(),
            row.storedActiveCount()
        );
        return new DomainRawSnapshot(
            row.couponId(),
            new ConsistencyRawSnapshot(
                values, redis.observation(), new SourceObservation(SourceStatus.VALID, observedAt)),
            row.lastIssuedAt(),
            row.lastIssuedAt() != null ? SourceStatus.VALID : SourceStatus.PENDING
        );
    }

    private StockSnapshot redisStock(CouponRow coupon) {
        if (redisTemplate == null) {
            log.warn("engineVersion={} 인데 Redis 통로가 없다.", properties.engineVersion());
            return new StockSnapshot(coupon.couponId(), null, SourceStatus.UNAVAILABLE,
                null, SourceStatus.UNAVAILABLE);
        }
        try {
            // V2·V3 에서 판매 여부를 정하는 것은 DB 행이 아니라 Redis 카운터다.
            Long remaining = number(key(properties.remainingKey(), coupon.couponId()));
            Long queueLength = size(key(properties.queueKey(), coupon.couponId()));
            return new StockSnapshot(
                coupon.couponId(),
                remaining, remaining == null ? SourceStatus.PENDING : SourceStatus.VALID,
                // 대기열 키가 없으면 "길이 0" 인지 "아직 모른다" 인지 구분할 수 없다. 0 으로
                // 단정하면 대기가 걸린 순간에도 화면이 평온해 보인다.
                queueLength, queueLength == null ? SourceStatus.PENDING : SourceStatus.VALID);
        } catch (RuntimeException exception) {
            log.warn("Redis 재고 조회에 실패했다. 값 대신 UNAVAILABLE 을 내보낸다.", exception);
            return new StockSnapshot(coupon.couponId(), null, SourceStatus.UNAVAILABLE,
                null, SourceStatus.UNAVAILABLE);
        }
    }

    /**
     * V1 에는 대기열 자체가 없다. 재고를 못 읽어도, 회차를 못 찾아도 대기열은 여전히 "해당 없음"
     * 이다. 없는 기능을 장애로 표시하면 화면이 장애색을 칠한다.
     */
    private SourceStatus queueStatusFor(SourceStatus status) {
        return properties.engineVersion() == EngineVersion.V1 ? SourceStatus.N_A : status;
    }

    /**
     * 관측 대상 회차를 못 찾았을 때의 상태.
     *
     * <p>회차를 <b>박았는데</b> 그 행이 없으면 오타이거나 지운 회차다 — 사람이 명시한 대상이
     * 없는 것이라 고장으로 본다. 이걸 PENDING 으로 두면 gap 과 재고가 측정 내내 빈 채로 있는데
     * 화면은 "준비 중" 으로만 보여서, 회차가 끝난 뒤에야 알아챈다.
     *
     * <p>반대로 회차를 박지 않은 상태에서 아직 아무 회차도 없는 것은 측정 전 대기라 고장이 아니다.
     */
    private SourceStatus missingCouponStatus() {
        if (properties.couponId() == null) {
            return SourceStatus.PENDING;
        }
        // 레벨을 WARN 으로 두는 이유 — 1초 주기라 ERROR 로 두면 같은 줄이 초당 하나씩 쌓인다.
        // 지속 여부 판단과 ERROR 승격은 registrar 의 연속 실패 카운터가 한다.
        log.warn("설정한 관측 대상 회차가 존재하지 않는다: couponId={}."
            + " observation.domain-gauge.coupon-id 를 확인한다.", properties.couponId());
        return SourceStatus.UNAVAILABLE;
    }

    private CouponRow queryCoupon() {
        List<CouponRow> rows = properties.couponId() == null
            ? observationJdbcTemplate.query(LATEST_COUPON_SQL, ConsistencyRawValueReader::mapCoupon,
                timeProvider.now())
            : observationJdbcTemplate.query(FIXED_COUPON_SQL, ConsistencyRawValueReader::mapCoupon,
                properties.couponId());
        return rows.isEmpty() ? null : rows.getFirst();
    }

    private AggregateRow queryAggregate(long couponId) {
        List<AggregateRow> rows = observationJdbcTemplate.query(
            AGGREGATE_SQL,
            (rs, rowNum) -> {
                LocalDateTime lastIssuedAt = rs.getObject("last_issued_at", LocalDateTime.class);
                return new AggregateRow(
                    couponId,
                    rs.getLong("total_quantity"),
                    rs.getLong("active_count"),
                    rs.getLong("db_active_count"),
                    rs.getLong("db_issued_ever_count"),
                    // 컬럼은 UTC 로 저장된다. JVM 기본 시간대로 해석하면 quiet period 가 시간 단위로 틀어진다.
                    lastIssuedAt == null ? null : lastIssuedAt.toInstant(ZoneOffset.UTC)
                );
            },
            couponId, couponId
        );
        return rows.isEmpty() ? null : rows.getFirst();
    }

    private static CouponRow mapCoupon(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        return new CouponRow(
            rs.getLong("coupon_id"),
            rs.getObject("total_quantity", Long.class),
            rs.getObject("active_count", Long.class));
    }

    private RedisRow readRedisForConsistency(long couponId) {
        if (properties.engineVersion() == EngineVersion.V1) {
            return RedisRow.notApplicable();
        }
        if (redisTemplate == null) {
            return RedisRow.unavailable();
        }
        // 값의 기준 시각은 요청을 보낸 순간이다. 응답을 받은 뒤에 찍으면 왕복 시간이 시차에서
        // 빠져, 실제로 300ms 벌어진 수집이 "시차 없음(0)" 으로 보고된다.
        Instant requestedAt = timeProvider.instant();
        try {
            List<?> result = redisTemplate.execute(CONSISTENCY_SCRIPT, List.of(
                key(properties.remainingKey(), couponId),
                key(properties.issuedEverKey(), couponId),
                key(properties.memberEverKey(), couponId)));
            if (result == null || result.size() != 3) {
                return RedisRow.unavailable();
            }
            Long remaining = toLong(result.get(0));
            Long issuedEver = toLong(result.get(1));
            Long memberEver = toLong(result.get(2));
            if (remaining == null || issuedEver == null || memberEver == null) {
                // 키가 아직 안 만들어진 예열 구간이다. 이때의 0 은 "다 팔렸다" 로 읽힌다.
                return RedisRow.pending();
            }
            return new RedisRow(
                new SourceObservation(SourceStatus.VALID, requestedAt),
                remaining, issuedEver, memberEver);
        } catch (RuntimeException exception) {
            log.warn("Redis 정합성 조회에 실패했다. 값 대신 UNAVAILABLE 을 내보낸다.", exception);
            return RedisRow.unavailable();
        }
    }

    private String key(String template, long couponId) {
        return DomainGaugeProperties.resolve(template, couponId);
    }

    private Long number(String key) {
        return toLong(redisTemplate.opsForValue().get(key));
    }

    private Long size(String key) {
        DataType type = redisTemplate.type(key);
        return switch (type) {
            case SET -> redisTemplate.opsForSet().size(key);
            case ZSET -> redisTemplate.opsForZSet().size(key);
            case LIST -> redisTemplate.opsForList().size(key);
            case STRING -> number(key);
            // 키가 없는 것(NONE)은 예열이고, 엉뚱한 자료형은 키를 잘못 가리킨 설정이다.
            case NONE -> null;
            default -> throw new InvalidRedisValueException(
                "대기열·집합 키가 예상 밖 자료형이다: type=" + type);
        };
    }

    /**
     * 값이 <b>없는 것</b>과 값이 <b>잘못된 것</b>을 가른다.
     *
     * <p>키가 아직 없는 것은 예열이라 곧 값이 나오지만, 숫자가 아닌 값이 들어 있는 것은 키를
     * 잘못 가리킨 설정이라 영영 안 나온다. 둘을 같은 {@code null} 로 뭉개면 오설정이 "곧 나올
     * 것" 으로 보여 아무 경보도 걸리지 않는다.
     *
     * @return 값이 없으면 {@code null}
     * @throws InvalidRedisValueException 값이 있는데 숫자로 읽을 수 없는 경우
     */
    private static Long toLong(Object value) {
        if (value == null || Boolean.FALSE.equals(value)) {
            return null;
        }
        if (INVALID_TYPE_MARKER.equals(value)) {
            throw new InvalidRedisValueException("Redis 키가 크기를 잴 수 없는 자료형이다");
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(value.toString().trim());
        } catch (NumberFormatException exception) {
            // 값 자체는 남기지 않는다. 키가 설정으로 바뀔 수 있어 무엇이 담겨 있을지 모르고,
            // 진단에 필요한 것은 "숫자가 아니었다" 는 사실뿐이다.
            log.warn("Redis 값이 숫자가 아니다: type={}, length={}",
                value.getClass().getSimpleName(), value.toString().length());
            throw new InvalidRedisValueException("Redis 값을 숫자로 읽을 수 없다");
        }
    }

    /**
     * @param couponId 어느 회차를 보다 실패했는지; 회차를 못 정한 단계였으면 {@code null}.
     *     회차를 버리면 여러 회차를 연달아 측정할 때 어느 구간이 죽었는지 지표로 가를 수 없다.
     */
    private static DomainRawSnapshot unavailableDatabase(Long couponId) {
        SourceObservation unavailable = new SourceObservation(SourceStatus.UNAVAILABLE, null);
        // 상태가 UNAVAILABLE 이면 계산기가 값을 쓰지 않는다. 여기 0 은 계산에 안 쓰이는 자리 채움이다.
        return new DomainRawSnapshot(
            couponId,
            new ConsistencyRawSnapshot(zeroValues(), unavailable, unavailable),
            null, SourceStatus.UNAVAILABLE);
    }

    private static DomainRawSnapshot pending() {
        SourceObservation pending = new SourceObservation(SourceStatus.PENDING, null);
        return new DomainRawSnapshot(
            null,
            new ConsistencyRawSnapshot(zeroValues(), pending, pending),
            null, SourceStatus.PENDING);
    }

    private static ConsistencyRawValues zeroValues() {
        return new ConsistencyRawValues(0, 0, 0, 0, 0, 0, 0);
    }

    /**
     * 값이 잘못 들어 있는 상태. 호출부의 {@code catch (RuntimeException)} 이 이를 UNAVAILABLE 로
     * 바꾼다 — 예열({@code null})과 달리 기다린다고 해결되지 않는다.
     */
    private static final class InvalidRedisValueException extends RuntimeException {

        private InvalidRedisValueException(String message) {
            super(message);
        }
    }

    /** 재고 행이 없으면 수량 둘이 null 이다. 회차 자체는 존재한다. */
    private record CouponRow(long couponId, Long totalQuantity, Long storedActiveCount) {

        boolean hasStock() {
            return totalQuantity != null && storedActiveCount != null;
        }
    }

    private record AggregateRow(
        long couponId,
        long totalQuantity,
        long storedActiveCount,
        long dbActiveCount,
        long dbIssuedEverCount,
        Instant lastIssuedAt
    ) {
    }

    private record RedisRow(
        SourceObservation observation,
        Long remainingOrNull,
        Long issuedEverOrNull,
        Long memberEverOrNull
    ) {

        static RedisRow notApplicable() {
            return new RedisRow(new SourceObservation(SourceStatus.N_A, null), null, null, null);
        }

        static RedisRow unavailable() {
            return new RedisRow(new SourceObservation(SourceStatus.UNAVAILABLE, null), null, null, null);
        }

        static RedisRow pending() {
            return new RedisRow(new SourceObservation(SourceStatus.PENDING, null), null, null, null);
        }

        long remaining() {
            return remainingOrNull == null ? 0 : remainingOrNull;
        }

        long issuedEverCount() {
            return issuedEverOrNull == null ? 0 : issuedEverOrNull;
        }

        long memberEverCount() {
            return memberEverOrNull == null ? 0 : memberEverOrNull;
        }
    }

    /**
     * 1초마다 갱신되는 가벼운 값.
     *
     * @param couponId 관측 대상 회차; 회차를 못 정했으면 {@code null}
     * @param stockRemaining 재고 잔량; 값이 없으면 {@code null}
     * @param stockStatus 재고 원천 상태
     * @param queueLength 대기열 길이; 값이 없으면 {@code null}
     * @param queueStatus 대기열 원천 상태
     */
    public record StockSnapshot(
        Long couponId,
        Long stockRemaining,
        SourceStatus stockStatus,
        Long queueLength,
        SourceStatus queueStatus
    ) {
    }

    /**
     * 한 문장에서 읽은 정합성 원시값 묶음.
     *
     * @param couponId 관측 대상 회차; 회차를 못 정했으면 {@code null}
     * @param consistency 정합성 계산기에 그대로 넘기는 원시값과 원천 상태
     * @param lastSuccessfulIssueAt 마지막 발급 성공 시각; 아직 없으면 {@code null}
     * @param lastSuccessfulIssueStatus 마지막 발급 시각의 원천 상태
     */
    public record DomainRawSnapshot(
        Long couponId,
        ConsistencyRawSnapshot consistency,
        Instant lastSuccessfulIssueAt,
        SourceStatus lastSuccessfulIssueStatus
    ) {
    }
}
