package com.kafkick.infra.redis.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.attempt.AttemptLiveEntry;
import com.kafkick.core.observation.attempt.AttemptLivePage;
import com.kafkick.core.observation.attempt.AttemptLiveReader;
import com.kafkick.core.observation.attempt.AttemptLiveSink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import tools.jackson.databind.ObjectMapper;

/**
 * live 화면의 링 버퍼. Redis Stream 하나다.
 *
 * <h2>발급 경로는 이 클래스를 부르지 않는다</h2>
 *
 * 부르는 것은 {@code attempt-live} 컨슈머뿐이다. v1 의 전제가 "발급 경로에 Redis 미사용" 이고,
 * 관측이 그 전제를 깨면 측정 대상이 측정 도구 때문에 달라진다. 핫패스는 Kafka 를 한 번 태우고
 * 끝이며, XADD 는 그 뒤 컨슈머 스레드에서 일어난다.
 *
 * <h2>{@code MAXLEN ~} 는 근사다</h2>
 *
 * {@code ~} 를 붙이면 Redis 가 노드 경계에서만 잘라서 <b>실제 길이가 200 을 넘는다.</b>
 * 정확한 {@code MAXLEN} 을 쓰면 XADD 마다 정확히 한 건씩 지우느라 O(N) 이 붙는데, 이 경로는
 * 초당 수십~수백 건이 들어오는 자리라 그 비용을 낼 이유가 없다 — 화면이 보는 것은 최근 몇
 * 백 건이지 정확히 200 건이 아니다.
 *
 * <p>그 대가가 {@code droppedCount} 를 셀 수 없다는 것이다. 근거는
 * {@link AttemptLivePage} javadoc 에 있다.
 *
 * <h2>조회는 왕복 한 번이다 — 그것이 만료 판정의 정확성을 만든다</h2>
 *
 * 처음에는 head 를 따로 읽어 커서와 비교하고(만료 판정) 그 다음에 커서 이후를 읽었다.
 * <b>그 사이가 경쟁 구간이다.</b> 첫 호출 시점에 커서가 살아 있어도 두 번째 호출 전에 XADD 가
 * 트리밍을 돌리면 커서 직후 항목들이 이미 지워져 있고, 그때 응답은 {@code cursorExpired=false}
 * 로 나간다 — {@link AttemptLivePage} 가 "놓친 것이 있으면 이 플래그가 선다" 고 선언한 계약이
 * 정확히 그 인터리빙에서 깨진다. 부하 구간의 버퍼는 67ms 분량이라(DEC-04 의 산수) 왕복 1ms 에도
 * 상시 발생한다.
 *
 * <p>그래서 <b>커서를 포함해</b> 한 번만 읽는다. 커서 자신이 돌아오면 그 자리는 살아 있는
 * 것이고, 안 돌아오면 트림된 것이다. 판정과 읽기가 같은 XRANGE 안에 있으므로 사이가 없다.
 * 덤으로 왕복이 하나 줄고 {@code compareIds}·{@code parseId} 같은 ID 비교 코드가 통째로 사라졌다
 * — 그 코드가 잘못된 커서에서 {@code NumberFormatException} 을 던져 관제 화면을 500 으로
 * 죽이던 자리다(실측으로 7종 입력 전부 재현했다).
 *
 * <h2>항목은 JSON 한 필드다</h2>
 *
 * 필드를 여럿으로 펼치지 않는다. 펼치면 필드 이름이 쓰는 쪽과 읽는 쪽 두 군데에 문자열로
 * 흩어지고, 한쪽만 바뀌어도 예외 없이 그 값만 조용히 null 이 된다. 한 필드에 레코드를 통째로
 * 실으면 계약이 {@link AttemptLiveEntry} 타입 하나가 되어 컴파일러가 본다.
 */
public final class AttemptLiveStream implements AttemptLiveSink, AttemptLiveReader {

    /** 화면이 폴링하는 유일한 키. */
    public static final String STREAM_KEY = "attempt:live";

    /** 근사 상한. 1 초 폴링에 이 정도면 화면이 창을 놓치지 않는다. */
    public static final long MAX_ENTRIES = 200L;

    /** 항목을 싣는 유일한 필드 이름. */
    static final String ENTRY_FIELD = "entry";

    private static final String STREAM_START = "-";
    private static final String STREAM_END = "+";

    /**
     * Redis Stream ID 형식. {@code ms} 또는 {@code ms-seq} 이고 각 부분은 <b>부호 없는 64비트</b>다.
     *
     * <p>형식을 여기서 거르지 않으면 커서가 Redis 커맨드로 그대로 내려가 드라이버 예외가 되고,
     * 그것이 500 으로 나가면서 <b>공격자가 넣은 문자열이 스택트레이스와 함께 로그에 실린다</b>
     * (개행 포함 가능 — 로그 인젝션). 게다가 화면 입장에서는 커서를 지우는 방법이 없어 관제가
     * 통째로 멈춘다. 형식이 틀린 커서는 트림된 커서와 <b>같은 복구</b>(머리부터 다시 읽기)가
     * 맞으므로 같은 경로로 보낸다.
     *
     * <p><b>자릿수만 보면 안 된다.</b> 처음에는 {@code \d{1,20}} 으로 잡았는데, 부호 없는 64비트
     * 상한이 {@code 18446744073709551615} 라 같은 20자리인 {@code 99999999999999999999} 가
     * 그 위다. 정규식은 통과시키고 Redis 가 거부해서, 걸러 내려던 500 이 그 입력으로만 그대로
     * 남아 있었다. 그래서 모양은 정규식이 보고 <b>값의 범위는 파싱이</b> 본다.
     */
    private static final Pattern STREAM_ID = Pattern.compile("\\d{1,20}(-\\d{1,20})?");

    /** 정규식을 태우기 전의 길이 상한. 위 패턴은 41자를 넘을 수 없다. */
    private static final int MAX_CURSOR_LENGTH = 41;

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final Counter unreadable;

    public AttemptLiveStream(
            StringRedisTemplate redis, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.redis = Objects.requireNonNull(redis, "redis");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.unreadable = Counter.builder(DomainMeterNames.ATTEMPT_LIVE_UNREADABLE)
                .description("live 버퍼에서 풀지 못해 건너뛴 항목 수 (커서는 넘어갔다)")
                .register(meterRegistry);
    }

    @Override
    public void append(AttemptLiveEntry entry) {
        Objects.requireNonNull(entry, "entry");
        MapRecord<String, String, String> record = StreamRecords.newRecord()
                .in(STREAM_KEY)
                .ofMap(Map.of(ENTRY_FIELD, objectMapper.writeValueAsString(entry)));
        redis.opsForStream().add(record, XAddOptions.maxlen(MAX_ENTRIES).approximateTrimming(true));
    }

    @Override
    public AttemptLivePage readAfter(String afterCursor, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit 은 1 이상이어야 합니다: " + limit);
        }
        String cursor = normalize(afterCursor);
        if (cursor == null) {
            // 최초 조회이거나 형식이 깨진 커서다. 둘 다 머리부터 읽는다.
            // 화면에 실을 limit 건 + hasMore 탐침 한 건.
            return page(readFrom(STREAM_START, limit + 1), null, expiredForBrokenCursor(afterCursor), limit);
        }

        // 커서를 포함해 읽는다. 커서 자신이 돌아오는지가 만료 판정이다 — 판정과 읽기가 한 왕복
        // 안에 있어야 그 사이에 트리밍이 끼어들 수 없다.
        // 커서 자신 + limit 건 + hasMore 탐침 한 건. 커서가 살아 있으면 아래에서 그 한 건을
        // 잘라 내므로, 여기서 +2 를 하지 않으면 커서 경로의 hasMore 가 영구히 false 가 된다.
        List<MapRecord<String, Object, Object>> fromCursor = readFrom(cursor, limit + 2);
        if (fromCursor.isEmpty()) {
            // 커서 이후가 비었다. 새 항목이 없는 것과 커서가 트림됐는데 그 뒤로도 아무것도 안
            // 들어온 것을 구분할 방법이 없다. 둘 중 "만료" 라고 말하는 쪽이 더 나쁘다 —
            // 조용한 구간마다 화면이 유실 배지를 단다.
            return new AttemptLivePage(List.of(), cursor, false, false);
        }
        RecordId firstId = fromCursor.get(0).getId();
        boolean cursorAlive = firstId != null && cursor.equals(firstId.getValue());
        List<MapRecord<String, Object, Object>> tail =
                cursorAlive ? fromCursor.subList(1, fromCursor.size()) : fromCursor;
        return page(tail, cursor, !cursorAlive, limit);
    }

    /**
     * 형식이 깨진 커서였는지. 화면에 <b>말은 해 준다</b> — 조용히 처음부터 주면 화면은 자기가
     * 보낸 커서가 무시된 줄 모르고 같은 값을 계속 보낸다.
     */
    private static boolean expiredForBrokenCursor(String afterCursor) {
        return afterCursor != null && !afterCursor.isBlank();
    }

    /**
     * {@code from} 부터 <b>포함해서</b> 정확히 {@code count} 건을 읽는다.
     *
     * <p><b>여기서는 아무것도 더하지 않는다.</b> 예전에는 이 메서드가 몰래 {@code +1} 을 했고
     * 호출부도 각자 더해서, 커서 경로는 {@code limit + 2} 를 머리 경로는 {@code limit + 1} 을
     * 읽었다. 결과는 우연히 맞았지만 "몇 건을 읽는가" 의 규약이 호출부마다 달랐다.
     *
     * <p>두 경로가 필요한 여유분이 실제로 다르다 — 머리 경로는 {@code hasMore} 탐침 한 건,
     * 커서 경로는 거기에 커서 자신 한 건이 더 필요하다(살아 있으면 잘라 낸다). 그래서 계산을
     * 호출부에 두고 이유를 그 자리에 적는다. 안쪽으로 몰면 커서 경로의 {@code hasMore} 가
     * 영구히 {@code false} 가 된다.
     */
    private List<MapRecord<String, Object, Object>> readFrom(String from, int count) {
        List<MapRecord<String, Object, Object>> records = redis.opsForStream()
                .range(STREAM_KEY, Range.closed(from, STREAM_END), Limit.limit().count(count));
        return records == null ? List.of() : records;
    }

    private AttemptLivePage page(
            List<MapRecord<String, Object, Object>> records,
            String requestedCursor,
            boolean cursorExpired,
            int limit
    ) {
        boolean hasMore = records.size() > limit;
        List<MapRecord<String, Object, Object>> page = hasMore
                ? records.subList(0, limit)
                : records;

        List<AttemptLiveEntry> entries = new ArrayList<>(page.size());
        String lastId = requestedCursor;
        for (MapRecord<String, Object, Object> record : page) {
            // ⚠️ 커서는 파싱 성공 여부와 <b>무관하게</b> 전진한다. 건너뛴 항목에서 커서를 멈추면,
            //    한 페이지가 전부 풀리지 않을 때 다음 폴링이 같은 구간을 다시 읽는다 — 그 항목들이
            //    트림돼 나갈 때까지 화면이 완전히 정지한다. 배포 직후, 즉 가장 봐야 하는 구간에서
            //    딱 그렇게 된다. 그 자리는 "읽지 못했지만 소비한 것" 이다.
            if (record.getId() != null) {
                lastId = record.getId().getValue();
            }
            AttemptLiveEntry entry = readEntry(record);
            if (entry != null) {
                entries.add(entry);
            }
        }
        return new AttemptLivePage(entries, lastId, hasMore, cursorExpired);
    }

    /**
     * 한 항목을 읽는다. <b>풀리지 않는 항목은 건너뛰되 센다.</b>
     *
     * <p>버퍼에는 배포 경계를 사이에 둔 두 형식이 함께 앉을 수 있다. 한 건 때문에 페이지 전체를
     * 500 으로 떨어뜨리면, 그 항목이 트림돼 나갈 때까지 관제 화면이 통째로 죽는다 — 배포 직후
     * 가장 봐야 하는 구간이다.
     *
     * <p>세지 않으면 그 상황이 <b>어떤 신호도 내지 않는다.</b> 쓰기는 정상이라
     * {@code append.failures} 는 0 이고, 트림도 없어 {@code cursorExpired} 도 거짓이다. 화면은
     * 그냥 항목이 좀 적을 뿐이고 운영자는 그것을 정상으로 읽는다.
     */
    private AttemptLiveEntry readEntry(MapRecord<String, Object, Object> record) {
        Object raw = record.getValue().get(ENTRY_FIELD);
        if (raw == null) {
            unreadable.increment();
            return null;
        }
        try {
            return objectMapper.readValue(raw.toString(), AttemptLiveEntry.class);
        } catch (RuntimeException malformed) {
            unreadable.increment();
            return null;
        }
    }

    /**
     * 커서를 다듬고 형식을 확인한다. 형식이 아니면 {@code null} — 커서가 없는 것과 같이 취급한다.
     *
     * <p>예외를 던지지 않는 이유는 이 값이 <b>화면이 들고 있던 값</b>이기 때문이다. 이전 배포의
     * 커서를 localStorage 에서 그대로 보내는 경우가 정상 경로에 있고, 그때 400 이나 500 을 주면
     * 화면은 커서를 지우는 방법을 모른 채 영구히 같은 오류를 받는다. 만료와 같은 복구가 맞다.
     */
    private static String normalize(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return null;
        }
        String trimmed = cursor.trim();
        if (trimmed.length() > MAX_CURSOR_LENGTH || !STREAM_ID.matcher(trimmed).matches()) {
            return null;
        }
        return withinUnsignedRange(trimmed) ? trimmed : null;
    }

    /**
     * 두 부분이 모두 부호 없는 64비트 안인지 본다.
     *
     * <p>{@code Long.parseUnsignedLong} 이 상한을 정확히 안다 — 자릿수로 근사하면 20자리
     * 경계에서 틀린다. 여기서 안 걸러 낸 값은 Redis 가 {@code ERR Invalid stream ID} 로
     * 거부하고, 그 예외가 관제 화면의 500 이 된다.
     */
    private static boolean withinUnsignedRange(String streamId) {
        int dash = streamId.indexOf('-');
        String millis = dash < 0 ? streamId : streamId.substring(0, dash);
        String sequence = dash < 0 ? "0" : streamId.substring(dash + 1);
        try {
            Long.parseUnsignedLong(millis);
            Long.parseUnsignedLong(sequence);
            return true;
        } catch (NumberFormatException outOfRange) {
            return false;
        }
    }
}
