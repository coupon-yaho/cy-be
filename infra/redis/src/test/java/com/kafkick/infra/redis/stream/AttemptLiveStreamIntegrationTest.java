package com.kafkick.infra.redis.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.attempt.AttemptLiveEntry;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.attempt.AttemptLivePage;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 실제 Redis 에 태운다. <b>대역으로는 아무것도 증명되지 않는 것들이 있다.</b>
 *
 * <ul>
 *   <li>{@code Range.leftOpen} 이 정말 XRANGE 의 배타 경계 {@code (} 로 번역되는지. 포함 경계로
 *       번역되면 폴링마다 마지막 항목이 한 번씩 더 나가고, 화면에는 중복이 쌓인다. mock 은
 *       그 번역을 아예 실행하지 않는다.</li>
 *   <li>{@code MAXLEN ~} 가 실제로 자르는지, 그리고 정확히 200 이 아닌지.</li>
 *   <li>{@code Instant} 가 이 매퍼로 왕복하는지. 직렬화만 되고 역직렬화가 안 되면 화면의
 *       시각 칸만 조용히 빈다.</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
class AttemptLiveStreamIntegrationTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static LettuceConnectionFactory connectionFactory;
    private static StringRedisTemplate redis;
    private static AttemptLiveStream stream;
    private static SimpleMeterRegistry meters;

    @BeforeAll
    static void startRedis() {
        connectionFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        redis = new StringRedisTemplate(connectionFactory);
        redis.afterPropertiesSet();
        ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
        meters = new SimpleMeterRegistry();
        stream = new AttemptLiveStream(redis, objectMapper, meters);
    }

    @AfterAll
    static void stopRedis() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @BeforeEach
    void clearRedis() {
        try (RedisConnection connection = connectionFactory.getConnection()) {
            connection.serverCommands().flushDb();
        }
    }

    /** 커서 경계가 배타인지. 포함이면 마지막 항목이 매 폴링마다 다시 나온다. */
    @Test
    void doesNotReturnTheCursorItemAgain() {
        stream.append(entry(1L));
        stream.append(entry(2L));
        stream.append(entry(3L));

        AttemptLivePage first = stream.readAfter(null, 2);
        assertThat(first.entries()).extracting(AttemptLiveEntry::memberId).containsExactly(1L, 2L);
        assertThat(first.hasMore()).isTrue();

        AttemptLivePage second = stream.readAfter(first.nextCursor(), 2);
        assertThat(second.entries()).extracting(AttemptLiveEntry::memberId)
                .as("커서 항목이 다시 나오면 배타 경계가 아니다")
                .containsExactly(3L);
        assertThat(second.hasMore()).isFalse();
    }

    /**
     * <b>살아 있는 커서 뒤에도 {@code hasMore} 가 선다.</b>
     *
     * <p>커서 경로는 커서 자신이 한 자리를 먹는다. 읽는 건수를 {@code limit + 1} 로만 잡으면
     * 잘라 낸 뒤 남는 것이 정확히 {@code limit} 이라 {@code hasMore} 가 <b>영구히 false</b> 가
     * 된다 — 화면은 뒤에 이벤트가 밀려 있어도 "다 봤다" 로 읽는다.
     *
     * <p>기존 테스트로는 이 결손이 안 잡혔다. 커서 경로에서 {@code hasMore=true} 를 기대하는
     * 단언이 하나도 없었기 때문이다(만료 경로에만 있었다).
     */
    @Test
    void reportsHasMoreOnTheCursorPathToo() {
        for (int i = 1; i <= 5; i++) {
            stream.append(entry(i));
        }

        AttemptLivePage first = stream.readAfter(null, 2);
        assertThat(first.entries()).extracting(AttemptLiveEntry::memberId).containsExactly(1L, 2L);

        AttemptLivePage second = stream.readAfter(first.nextCursor(), 2);

        assertThat(second.entries()).extracting(AttemptLiveEntry::memberId).containsExactly(3L, 4L);
        assertThat(second.hasMore()).as("뒤에 5번이 남아 있다").isTrue();

        AttemptLivePage third = stream.readAfter(second.nextCursor(), 2);
        assertThat(third.entries()).extracting(AttemptLiveEntry::memberId).containsExactly(5L);
        assertThat(third.hasMore()).isFalse();
    }

    /** 새 항목이 없으면 빈 페이지이고 커서는 그대로다. */
    @Test
    void returnsAnEmptyPageAndKeepsTheCursorWhenNothingIsNew() {
        stream.append(entry(1L));
        AttemptLivePage first = stream.readAfter(null, 10);

        AttemptLivePage second = stream.readAfter(first.nextCursor(), 10);

        assertThat(second.entries()).isEmpty();
        assertThat(second.nextCursor()).isEqualTo(first.nextCursor());
        assertThat(second.cursorExpired()).isFalse();
    }

    /**
     * 이 티켓의 인수 조건 — <b>커서 만료 시 빈 응답이 아니라 현재 첫 항목부터.</b>
     *
     * <p>커서를 트림 구간 밖의 값으로 직접 만든다. {@code 0-1} 은 어떤 실제 항목보다도 작다.
     */
    @Test
    void restartsFromTheHeadWhenTheCursorIsOutsideTheBuffer() {
        stream.append(entry(1L));
        stream.append(entry(2L));

        AttemptLivePage page = stream.readAfter("0-1", 10);

        assertThat(page.cursorExpired()).isTrue();
        assertThat(page.entries()).extracting(AttemptLiveEntry::memberId).containsExactly(1L, 2L);
    }

    /** 빈 스트림에서는 만료로 판정하지 않는다 — 기동 직후 첫 폴링이 유실 경고를 달면 안 된다. */
    @Test
    void doesNotCallAnEmptyStreamExpired() {
        AttemptLivePage page = stream.readAfter("0-1", 10);

        assertThat(page.cursorExpired()).isFalse();
        assertThat(page.entries()).isEmpty();
    }

    /**
     * {@code MAXLEN ~} 는 자르되 정확히 {@value AttemptLiveStream#MAX_ENTRIES} 가 아니다.
     *
     * <p>이 테스트가 고정하는 것은 "잘린다" 와 "정확하지 않다" 두 가지다. 정확한 길이를
     * 단언하면 Redis 버전이나 노드 크기가 바뀔 때마다 깨지고, 상한 없이 두면 {@code ~} 를
     * 실수로 빼도 안 잡힌다.
     */
    @Test
    void trimsApproximatelyNotExactly() {
        for (int i = 0; i < 1_500; i++) {
            stream.append(entry(i + 1));
        }

        Long size = redis.opsForStream().size(AttemptLiveStream.STREAM_KEY);

        assertThat(size).isNotNull();
        assertThat(size).as("잘리긴 해야 한다").isLessThan(1_500L);
        // 실측 — redis:7.4-alpine · 기본 stream-node-max-entries(100) 에서 1,500 건을 넣으면 204 다.
        // 정확 trimming(approximateTrimming(false))이면 정확히 200 이므로, 이 단언이
        // isGreaterThanOrEqualTo 였을 때는 ~ 를 빼도 초록이었다(일부러 빼서 확인했다).
        //
        // ⚠️ 이 값은 노드 경계에 달려 있다. Redis 가 노드 크기 정책을 바꾸면 다시 재야 한다 —
        //    그때 깨지는 것이 맞다. 조용히 정확 trimming 으로 돌아가는 것보다 낫다.
        assertThat(size).as("근사 trimming 이라 상한을 넘겨서 남는다")
                .isGreaterThan(AttemptLiveStream.MAX_ENTRIES);
    }

    /** 모든 필드가 왕복한다. 특히 {@code Instant} 와 두 enum. */
    @Test
    void roundTripsEveryFieldThroughRedis() {
        AttemptLiveEntry written = new AttemptLiveEntry(
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                EventType.ISSUE_RESULT, 101L, 201L, 301L, "ABCD1234", Grade.GOLD,
                409, ReasonCode.ALREADY_ISSUED, null, null, true,
                Instant.parse("2026-08-25T01:02:03.123456Z"),
                Instant.parse("2026-08-25T01:02:03.654321Z"));
        stream.append(written);

        List<AttemptLiveEntry> read = stream.readAfter(null, 10).entries();

        assertThat(read).containsExactly(written);
    }

    /** 풀리지 않는 항목 하나가 페이지 전체를 죽이지 않는다. 그리고 <b>세어진다.</b> */
    @Test
    void skipsUnreadableEntriesInsteadOfFailingThePage() {
        double before = unreadableCount();
        stream.append(entry(1L));
        appendRaw("{not json");
        stream.append(entry(2L));

        AttemptLivePage page = stream.readAfter(null, 10);

        assertThat(page.entries()).extracting(AttemptLiveEntry::memberId).containsExactly(1L, 2L);
        assertThat(unreadableCount() - before)
                .as("세지 않으면 형식 사고가 어떤 신호도 안 낸다")
                .isEqualTo(1.0);
    }

    /**
     * <b>한 페이지가 통째로 안 풀려도 커서는 전진한다.</b>
     *
     * <p>커서를 파싱 성공한 항목에서만 전진시키면, 구형식 항목이 {@code limit} 건 연속으로
     * 앉아 있을 때 다음 폴링이 같은 구간을 다시 읽는다 — 그 항목들이 트림돼 나갈 때까지
     * 화면이 완전히 정지한다. 배포 직후, 즉 가장 봐야 하는 구간에서 그렇게 된다.
     */
    @Test
    void advancesTheCursorEvenWhenEveryEntryInThePageIsUnreadable() {
        appendRaw("{not json 1");
        appendRaw("{not json 2");
        stream.append(entry(7L));

        AttemptLivePage stalled = stream.readAfter(null, 2);
        assertThat(stalled.entries()).isEmpty();
        assertThat(stalled.hasMore()).isTrue();
        assertThat(stalled.nextCursor())
                .as("여기서 커서가 안 움직이면 화면이 그 자리에 영구히 선다")
                .isNotNull();

        AttemptLivePage next = stream.readAfter(stalled.nextCursor(), 2);
        assertThat(next.entries()).extracting(AttemptLiveEntry::memberId).containsExactly(7L);
    }

    /**
     * 형식이 깨진 커서는 500 이 아니라 만료와 같은 복구다.
     *
     * <p>실측 — 이 방어 전에는 {@code abc} · {@code 1-2-3} · {@code -} · {@code +} · {@code 5-}
     * · {@code 1 2} · 20자리 초과 숫자 <b>7종 전부</b>가 {@code NumberFormatException} 으로
     * 나가 관제 화면이 500 이었고, 그 문자열이 스택트레이스와 함께 로그에 실렸다.
     */
    @Test
    void treatsAMalformedCursorAsExpiredInsteadOfFailing() {
        stream.append(entry(1L));

        for (String malformed : new String[] {
                "abc", "1-2-3", "99999999999999999999999", "-", "+", "5-", "1 2", "1-0; FLUSHALL"}) {
            AttemptLivePage page = stream.readAfter(malformed, 10);

            assertThat(page.cursorExpired()).as("커서 [%s]", malformed).isTrue();
            assertThat(page.entries()).extracting(AttemptLiveEntry::memberId).containsExactly(1L);
        }
    }

    /**
     * 만료 판정과 읽기가 <b>한 왕복</b>이다.
     *
     * <p>커서가 트림된 상태를 실제로 만든 뒤, 응답이 만료 플래그와 현재 항목을 함께 주는지 본다.
     * 판정을 별도 XRANGE 로 하면 그 사이에 트리밍이 끼어들어 "놓쳤는데 안 놓쳤다고 말하는"
     * 응답이 나온다 — 부하 구간의 버퍼가 67ms 분량이라 상시 발생한다.
     */
    @Test
    void reportsExpiryForACursorThatWasTrimmedAway() {
        stream.append(entry(1L));
        AttemptLivePage first = stream.readAfter(null, 10);
        String oldCursor = first.nextCursor();

        // 그 커서를 버퍼 밖으로 밀어낸다.
        for (int i = 0; i < 1_500; i++) {
            stream.append(entry(i + 100));
        }

        AttemptLivePage page = stream.readAfter(oldCursor, 5);

        assertThat(page.cursorExpired()).isTrue();
        assertThat(page.entries()).hasSize(5);
        assertThat(page.hasMore()).isTrue();
    }

    /** 살아 있는 커서는 만료가 아니다. 위 테스트만 두면 "항상 만료" 구현도 초록이다. */
    @Test
    void doesNotReportExpiryForALiveCursor() {
        stream.append(entry(1L));
        stream.append(entry(2L));

        AttemptLivePage first = stream.readAfter(null, 1);
        AttemptLivePage second = stream.readAfter(first.nextCursor(), 1);

        assertThat(first.cursorExpired()).isFalse();
        assertThat(second.cursorExpired()).isFalse();
        assertThat(second.entries()).extracting(AttemptLiveEntry::memberId).containsExactly(2L);
    }

    private static void appendRaw(String json) {
        redis.opsForStream().add(AttemptLiveStream.STREAM_KEY,
                java.util.Map.of(AttemptLiveStream.ENTRY_FIELD, json));
    }

    private static double unreadableCount() {
        return meters.find(DomainMeterNames.ATTEMPT_LIVE_UNREADABLE).counters().stream()
                .mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
    }

    private static AttemptLiveEntry entry(long memberId) {
        return new AttemptLiveEntry(
                UUID.randomUUID(), EventType.ISSUE_ATTEMPT, memberId, 201L, null, null,
                Grade.GOLD, null, null, null, null, false,
                Instant.parse("2026-08-25T00:00:00Z"), Instant.parse("2026-08-25T00:00:01Z"));
    }
}
