package com.kafkick.infra.redis.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.admin.preparation.V2AdminPreparationReader;
import com.kafkick.core.admin.preparation.V2PreparationSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/** 실제 Redis 7.4에서 V2 워밍업 네 키와 게이트 meta의 준비 판정 계약을 검증합니다. */
@Testcontainers(disabledWithoutDocker = true)
class RedisV2AdminPreparationReaderIntegrationTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static final Instant SNAPSHOT = Instant.parse("2026-08-29T09:00:00Z");
    private static final Instant OPENS_AT = Instant.parse("2026-08-29T10:00:00Z");
    private static final Instant CLOSES_AT = Instant.parse("2026-08-29T11:00:00Z");
    private static final String GRADE_MASK = "3";
    private static final String TOTAL_QUANTITY = "100";

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private RedisV2AdminPreparationReader reader;

    /** Testcontainers Redis에 Spring Data 연결을 구성합니다. */
    @BeforeAll
    static void startRedis() {
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        factory.afterPropertiesSet();
        factory.start();
        redis = new StringRedisTemplate(factory);
    }

    /** 테스트 전용 Redis 연결 자원을 반환합니다. */
    @AfterAll
    static void stopRedis() {
        factory.destroy();
    }

    /** 회차별 키 상태가 테스트 사이에 섞이지 않도록 DB를 비웁니다. */
    @BeforeEach
    void reset() {
        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
        reader = new RedisV2AdminPreparationReader(redis);
    }

    /** 발급 이력 0건에서 Redis가 만들지 않는 빈 Hash를 오준비로 판정하지 않는지 검증합니다. */
    @Test
    @DisplayName("issued_ever가 0이면 issued Hash가 없어도 워밍업과 게이트가 준비된다")
    void acceptsMissingIssuedHashWhenIssuedEverIsZero() {
        writeMeta(10L);
        writeCounters(10L, "100", Map.of(), "0");

        V2PreparationSource result = read(request(10L)).get(10L);

        assertThat(result).isEqualTo(
                new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT));
    }

    /** 워밍업이 시작되지 않은 예약 회차를 확정 실패로 오인하지 않는지 검증합니다. */
    @Test
    @DisplayName("네 Redis 키가 모두 없으면 PENDING이다")
    void mapsAllMissingKeysToPending() {
        V2PreparationSource result = read(request(10L)).get(10L);

        assertThat(result).isEqualTo(
                new V2PreparationSource(null, null, SourceStatus.PENDING, null));
    }

    /** 한 축만 완성된 부분 상태가 준비 완료로 합쳐지지 않는지 검증합니다. */
    @Test
    @DisplayName("부분 키 상태는 완성된 축만 준비로 보존한다")
    void reportsReadinessPerAxisForPartialState() {
        writeCounters(10L, "100", Map.of(), "0");
        writeMeta(11L);

        Map<Long, V2PreparationSource> result = read(List.of(request(10L), request(11L)));

        assertThat(result.get(10L)).isEqualTo(
                new V2PreparationSource(true, false, SourceStatus.VALID, SNAPSHOT));
        assertThat(result.get(11L)).isEqualTo(
                new V2PreparationSource(false, true, SourceStatus.VALID, SNAPSHOT));
    }

    /** 누적 발급 수와 회원 Hash 크기가 갈리면 워밍업만 실패하는지 검증합니다. */
    @Test
    @DisplayName("issued Hash 크기와 issued_ever가 다르면 워밍업만 실패한다")
    void rejectsIssuedCardinalityMismatch() {
        writeMeta(10L);
        writeCounters(10L, "99", Map.of("1", "D|1|token|key"), "2");

        V2PreparationSource result = read(request(10L)).get(10L);

        assertThat(result).isEqualTo(
                new V2PreparationSource(false, true, SourceStatus.VALID, SNAPSHOT));
    }

    /** DB active_count로 확정한 잔여재고보다 작은 Redis stock을 준비 완료로 숨기지 않는지 검증합니다. */
    @Test
    @DisplayName("DB 기대 잔여재고와 다른 stock은 워밍업 실패다")
    void rejectsStockDifferentFromExpectedRemainingQuantity() {
        writeMeta(10L);
        writeCounters(10L, "99", Map.of(), "0");

        V2PreparationSource result = read(request(10L)).get(10L);

        assertThat(result).isEqualTo(
                new V2PreparationSource(false, true, SourceStatus.VALID, SNAPSHOT));
    }

    /** Hash 크기가 맞아도 실제 발급 스크립트가 해석하지 못하는 값을 준비로 판정하지 않는지 검증합니다. */
    @Test
    @DisplayName("issued Hash의 파손 값은 워밍업 실패다")
    void rejectsCorruptIssuedValue() {
        writeMeta(10L);
        writeCounters(10L, "100", Map.of("1", "BROKEN"), "1");

        V2PreparationSource result = read(request(10L)).get(10L);

        assertThat(result).isEqualTo(
                new V2PreparationSource(false, true, SourceStatus.VALID, SNAPSHOT));
    }

    /** 실제 발급 코덱 값과 DB 기대 잔여재고가 맞으면 증분 스캔 뒤에도 준비 상태를 보존하는지 검증합니다. */
    @Test
    @DisplayName("정상 issued 값과 기대 잔여재고는 워밍업 준비다")
    void acceptsValidIssuedValueAndExpectedRemainingStock() {
        writeMeta(10L);
        writeCounters(10L, "99", Map.of("1", "D|1|token|key"), "1");

        V2PreparationSource result = read(request(10L, 99L)).get(10L);

        assertThat(result).isEqualTo(
                new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT));
    }

    /** DB 정본과 다른 meta가 워밍업 판정을 가리지 않고 게이트만 실패시키는지 검증합니다. */
    @Test
    @DisplayName("DB와 meta가 다르면 게이트만 준비 실패다")
    void rejectsMetaMismatchWithoutHidingWarmup() {
        writeMeta(10L);
        writeCounters(10L, "100", Map.of(), "0");
        redis.opsForHash().put(
                IssuanceKeys.of(10L).meta(), RedisIssuanceGate.META_TOTAL_QUANTITY, "99");

        V2PreparationSource result = read(request(10L)).get(10L);

        assertThat(result).isEqualTo(
                new V2PreparationSource(true, false, SourceStatus.VALID, SNAPSHOT));
    }

    /** 비정규 숫자와 총수량 상한 위반을 Redis 준비 완료로 정규화하지 않는지 검증합니다. */
    @Test
    @DisplayName("파손 숫자와 총수량을 넘는 stock은 워밍업 실패다")
    void rejectsNonCanonicalAndOutOfRangeCounters() {
        writeMeta(10L);
        writeCounters(10L, "01", Map.of(), "0");
        writeMeta(11L);
        writeCounters(11L, "101", Map.of(), "0");
        writeMeta(12L);
        writeCounters(12L, "100", Map.of(), " 0");
        writeMeta(13L);
        writeCounters(13L, "100", Map.of(), "-1");

        Map<Long, V2PreparationSource> result = read(List.of(
                request(10L), request(11L), request(12L), request(13L)));

        assertThat(result.values()).allSatisfy(source -> {
            assertThat(source.status()).isEqualTo(SourceStatus.VALID);
            assertThat(source.warmupReady()).isFalse();
            assertThat(source.gateReady()).isTrue();
        });
    }

    /** Redis 자료형 파손이 명령 오류로 batch 전체를 덮지 않고 해당 축 실패로 남는지 검증합니다. */
    @Test
    @DisplayName("잘못된 Redis 자료형은 해당 회차 준비 실패로 반환한다")
    void rejectsWrongRedisTypesPerCampaign() {
        writeMeta(10L);
        IssuanceKeys stockHash = IssuanceKeys.of(10L);
        redis.opsForHash().put(stockHash.stock(), "field", "100");
        redis.opsForValue().set(stockHash.issuedEver(), "0");

        IssuanceKeys metaString = IssuanceKeys.of(11L);
        redis.opsForValue().set(metaString.meta(), "OPEN");
        writeCounters(11L, "100", Map.of(), "0");

        Map<Long, V2PreparationSource> result = read(List.of(request(10L), request(11L)));

        assertThat(result.get(10L)).isEqualTo(
                new V2PreparationSource(false, true, SourceStatus.VALID, SNAPSHOT));
        assertThat(result.get(11L)).isEqualTo(
                new V2PreparationSource(true, false, SourceStatus.VALID, SNAPSHOT));
    }

    /** meta 필드 누락·상태·DB 비교값의 각 오류가 게이트 실패로 드러나는지 검증합니다. */
    @Test
    @DisplayName("불완전하거나 DB와 다른 meta는 게이트 준비 실패다")
    void rejectsIncompleteClosedAndMismatchedMeta() {
        writeMeta(10L);
        redis.opsForHash().delete(IssuanceKeys.of(10L).meta(), RedisIssuanceGate.META_CLOSE_AT);
        writeMeta(11L);
        redis.opsForHash().put(IssuanceKeys.of(11L).meta(), RedisIssuanceGate.META_STATUS, "CLOSED");
        writeMeta(12L);
        redis.opsForHash().put(IssuanceKeys.of(12L).meta(), RedisIssuanceGate.META_OPEN_AT, "1");
        writeMeta(13L);
        redis.opsForHash().put(IssuanceKeys.of(13L).meta(), RedisIssuanceGate.META_CLOSE_AT, "2");
        writeMeta(14L);
        redis.opsForHash().put(IssuanceKeys.of(14L).meta(), RedisIssuanceGate.META_GRADE_MASK, "4");
        for (long couponId = 10L; couponId <= 14L; couponId++) {
            writeCounters(couponId, "100", Map.of(), "0");
        }

        Map<Long, V2PreparationSource> result = read(List.of(
                request(10L), request(11L), request(12L), request(13L), request(14L)));

        assertThat(result.values()).allSatisfy(source -> {
            assertThat(source.warmupReady()).isTrue();
            assertThat(source.gateReady()).isFalse();
        });
    }

    /** pipeline 순서가 couponId 매핑 순서로 보존되고 호출자가 결과를 바꿀 수 없는지 검증합니다. */
    @Test
    @DisplayName("다회차 pipeline은 요청 순서의 불변 결과를 반환한다")
    void preservesRequestOrderAndReturnsImmutableMap() {
        writeMeta(20L);
        writeCounters(20L, "100", Map.of(), "0");
        writeMeta(10L);
        writeCounters(10L, "100", Map.of(), "0");

        Map<Long, V2PreparationSource> result = read(List.of(request(20L), request(10L)));

        assertThat(result.keySet()).containsExactly(20L, 10L);
        assertThatThrownBy(result::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    /** 단일 회차 요청을 같은 batch 조회 경계로 전달합니다. */
    private Map<Long, V2PreparationSource> read(V2AdminPreparationReader.Request request) {
        return read(List.of(request));
    }

    /** 지정한 순서의 요청을 고정 관측 시각으로 조회합니다. */
    private Map<Long, V2PreparationSource> read(List<V2AdminPreparationReader.Request> requests) {
        return reader.read(requests, SNAPSHOT);
    }

    /** 정상 DB 비교값을 가진 예약 회차 요청을 생성합니다. */
    private static V2AdminPreparationReader.Request request(long couponId) {
        return request(couponId, Long.parseLong(TOTAL_QUANTITY));
    }

    /** 정상 DB 비교값과 지정한 기대 잔여재고를 가진 예약 회차 요청을 생성합니다. */
    private static V2AdminPreparationReader.Request request(
            long couponId,
            long expectedRemainingQuantity
    ) {
        return new V2AdminPreparationReader.Request(
                couponId, CouponRoundStatus.SCHEDULED,
                OPENS_AT, CLOSES_AT, Integer.parseInt(GRADE_MASK),
                Long.parseLong(TOTAL_QUANTITY), expectedRemainingQuantity);
    }

    /** 실제 게이트가 쓰는 다섯 meta 필드를 정상 DB 비교값으로 저장합니다. */
    private static void writeMeta(long couponId) {
        redis.opsForHash().putAll(IssuanceKeys.of(couponId).meta(), Map.of(
                RedisIssuanceGate.META_STATUS, "OPEN",
                RedisIssuanceGate.META_OPEN_AT, Long.toString(OPENS_AT.toEpochMilli()),
                RedisIssuanceGate.META_CLOSE_AT, Long.toString(CLOSES_AT.toEpochMilli()),
                RedisIssuanceGate.META_GRADE_MASK, GRADE_MASK,
                RedisIssuanceGate.META_TOTAL_QUANTITY, TOTAL_QUANTITY));
    }

    /** 워밍업이 만드는 stock·issued·issued_ever 키를 주어진 값 그대로 저장합니다. */
    private static void writeCounters(
            long couponId,
            String stock,
            Map<String, String> issued,
            String issuedEver
    ) {
        IssuanceKeys keys = IssuanceKeys.of(couponId);
        redis.opsForValue().set(keys.stock(), stock);
        if (!issued.isEmpty()) {
            redis.opsForHash().putAll(keys.issued(), new LinkedHashMap<>(issued));
        }
        redis.opsForValue().set(keys.issuedEver(), issuedEver);
    }
}
