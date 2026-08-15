package com.kafkick.batch.replay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.verification.replay.IssuanceHistoryRecord;
import com.kafkick.core.verification.replay.IssuanceIdRange;
import com.kafkick.core.verification.replay.ReplayHistoryRepository;

/**
 * 저장소는 가짜로 둔다. 여기서 보려는 것은 SQL 이 아니라 <b>창을 어떻게 밀고 재시작 위치를
 * 어디에 두느냐</b>이고, 그건 실제 DB 없이도 전부 드러난다. SQL 자체는
 * ReplayHistoryJdbcAdapterTest 가 실컨테이너로 본다.
 */
class IssuanceHistoryGroupReaderTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);

    @Test
    @DisplayName("이력이 없으면 처음부터 끝이다")
    void readNothingWhenNoHistory() {
        IssuanceHistoryGroupReader reader = reader(new FakeHistories(), 10);
        reader.open(new ExecutionContext());

        assertThat(reader.read()).isNull();
    }

    @Test
    @DisplayName("발급건마다 묶어서 내보낸다")
    void groupByIssuance() {
        FakeHistories histories = new FakeHistories()
                .with(1L, issue(1L, 1L), use(2L, 1L))
                .with(2L, issue(3L, 2L));

        List<IssuanceHistoryGroup> groups = readAll(reader(histories, 10));

        assertThat(groups).hasSize(2);
        assertThat(groups.get(0).issuanceId()).isEqualTo(1L);
        assertThat(groups.get(0).histories()).hasSize(2);
        assertThat(groups.get(1).issuanceId()).isEqualTo(2L);
    }

    @Test
    @DisplayName("창을 여러 번 밀어 전부 읽는다 — 한 창에 다 안 들어와도 빠뜨리지 않는다")
    void slideWindowUntilExhausted() {
        FakeHistories histories = new FakeHistories()
                .with(1L, issue(1L, 1L))
                .with(2L, issue(2L, 2L))
                .with(3L, issue(3L, 3L))
                .with(4L, issue(4L, 4L))
                .with(5L, issue(5L, 5L));

        List<IssuanceHistoryGroup> groups = readAll(reader(histories, 2));

        assertThat(groups).extracting(IssuanceHistoryGroup::issuanceId)
                .containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(histories.requestedWindows)
                .containsExactly("1-2", "3-4", "5-5");
    }

    @Test
    @DisplayName("창은 발급건 식별자로 자른다 — 한 발급건의 이력이 두 창에 걸치지 않는다")
    void neverSplitOneIssuanceAcrossWindows() {
        FakeHistories histories = new FakeHistories()
                .with(1L, issue(1L, 1L), use(2L, 1L), cancelUse(3L, 1L))
                .with(2L, issue(4L, 2L));

        List<IssuanceHistoryGroup> groups = readAll(reader(histories, 1));

        assertThat(groups.get(0).histories()).hasSize(3);
        assertThat(groups.get(1).histories()).hasSize(1);
    }

    @Test
    @DisplayName("빈 창을 만나도 멈추지 않고 다음 창으로 넘어간다 — 식별자에 구멍이 있다")
    void skipEmptyWindow() {
        FakeHistories histories = new FakeHistories()
                .with(1L, issue(1L, 1L))
                .with(9L, issue(2L, 9L));

        List<IssuanceHistoryGroup> groups = readAll(reader(histories, 2));

        assertThat(groups).extracting(IssuanceHistoryGroup::issuanceId).containsExactly(1L, 9L);
    }

    @Test
    @DisplayName("훑을 범위는 처음 한 번만 구해 실행 컨텍스트에 박는다 — 다시 구하면 결정론이 깨진다")
    void freezeRangeOnFirstOpen() {
        FakeHistories histories = new FakeHistories().with(1L, issue(1L, 1L));
        ExecutionContext context = new ExecutionContext();

        reader(histories, 10).open(context);

        assertThat(context.getLong("replay.range.min")).isEqualTo(1L);
        assertThat(context.getLong("replay.range.max")).isEqualTo(1L);
        assertThat(histories.rangeCalls).isEqualTo(1);
    }

    @Test
    @DisplayName("재시작하면 범위를 다시 구하지 않고 저장된 것을 쓴다")
    void restoreRangeInsteadOfRecomputing() {
        FakeHistories histories = new FakeHistories()
                .with(1L, issue(1L, 1L))
                .with(2L, issue(2L, 2L));
        ExecutionContext context = new ExecutionContext();
        context.putLong("replay.range.min", 1L);
        context.putLong("replay.range.max", 2L);
        context.putLong("replay.window.start", 2L);

        List<IssuanceHistoryGroup> groups = readAll(reader(histories, 10), context);

        assertThat(histories.rangeCalls).isZero();
        assertThat(groups).extracting(IssuanceHistoryGroup::issuanceId).containsExactly(2L);
    }

    @Test
    @DisplayName("버퍼가 남아 있으면 그 창의 시작을 저장한다 — 다음 창을 저장하면 남은 묶음이 사라진다")
    void savePendingWindowWhileBufferRemains() {
        FakeHistories histories = new FakeHistories()
                .with(1L, issue(1L, 1L))
                .with(2L, issue(2L, 2L))
                .with(3L, issue(3L, 3L));
        IssuanceHistoryGroupReader reader = reader(histories, 3);
        ExecutionContext context = new ExecutionContext();
        reader.open(context);

        reader.read();
        reader.update(context);

        assertThat(context.getLong("replay.window.start")).isEqualTo(1L);
    }

    @Test
    @DisplayName("창을 다 비우면 다음 창의 시작을 저장한다")
    void saveNextWindowOnceDrained() {
        FakeHistories histories = new FakeHistories()
                .with(1L, issue(1L, 1L))
                .with(5L, issue(2L, 5L));
        IssuanceHistoryGroupReader reader = reader(histories, 2);
        ExecutionContext context = new ExecutionContext();
        reader.open(context);

        reader.read();
        reader.update(context);

        assertThat(context.getLong("replay.window.start")).isEqualTo(3L);
    }

    @Test
    @DisplayName("저장된 위치에서 다시 열면 앞의 창을 다시 읽지 않는다")
    void resumeFromSavedWindow() {
        FakeHistories histories = new FakeHistories()
                .with(1L, issue(1L, 1L))
                .with(2L, issue(2L, 2L))
                .with(3L, issue(3L, 3L));
        IssuanceHistoryGroupReader first = reader(histories, 1);
        ExecutionContext context = new ExecutionContext();
        first.open(context);
        first.read();
        first.update(context);
        first.close();

        List<IssuanceHistoryGroup> rest = readAll(reader(histories, 1), context);

        assertThat(rest).extracting(IssuanceHistoryGroup::issuanceId).containsExactly(2L, 3L);
    }

    @Test
    @DisplayName("죽은 창은 통째로 다시 읽는다 — 손실보다 중복이 낫다. 라이터가 UPSERT 다")
    void rereadWholeWindowAfterCrashMidWindow() {
        FakeHistories histories = new FakeHistories()
                .with(1L, issue(1L, 1L))
                .with(2L, issue(2L, 2L))
                .with(3L, issue(3L, 3L));
        IssuanceHistoryGroupReader crashed = reader(histories, 3);
        ExecutionContext context = new ExecutionContext();
        crashed.open(context);
        crashed.read();
        crashed.read();
        crashed.update(context);

        List<IssuanceHistoryGroup> afterRestart = readAll(reader(histories, 3), context);

        assertThat(afterRestart).extracting(IssuanceHistoryGroup::issuanceId)
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    @DisplayName("창 크기가 0 이하면 거부한다 — 창이 안 움직여 영원히 돈다")
    void rejectNonPositiveWindowSize() {
        assertThatThrownBy(() -> reader(new FakeHistories(), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("창 크기는 1 이상");
    }

    @Test
    @DisplayName("범위 끝이 Long 최대여도 창 계산이 넘치지 않는다")
    void surviveWindowOverflowAtLongMax() {
        FakeHistories histories = new FakeHistories()
                .with(Long.MAX_VALUE, issue(1L, Long.MAX_VALUE));

        List<IssuanceHistoryGroup> groups = readAll(reader(histories, Long.MAX_VALUE));

        assertThat(groups).extracting(IssuanceHistoryGroup::issuanceId)
                .containsExactly(Long.MAX_VALUE);
    }

    private static IssuanceHistoryGroupReader reader(FakeHistories histories, long windowSize) {
        return new IssuanceHistoryGroupReader(histories, AS_OF, windowSize);
    }

    private static List<IssuanceHistoryGroup> readAll(IssuanceHistoryGroupReader reader) {
        return readAll(reader, new ExecutionContext());
    }

    private static List<IssuanceHistoryGroup> readAll(
            IssuanceHistoryGroupReader reader,
            ExecutionContext context
    ) {
        reader.open(context);

        List<IssuanceHistoryGroup> groups = new ArrayList<>();
        IssuanceHistoryGroup group = reader.read();
        while (group != null) {
            groups.add(group);
            group = reader.read();
        }

        reader.close();
        return groups;
    }

    private static IssuanceHistoryRecord issue(long id, long issuanceId) {
        return new IssuanceHistoryRecord(id, issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(3));
    }

    private static IssuanceHistoryRecord use(long id, long issuanceId) {
        return new IssuanceHistoryRecord(id, issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF.minusHours(2));
    }

    private static IssuanceHistoryRecord cancelUse(long id, long issuanceId) {
        return new IssuanceHistoryRecord(id, issuanceId, IssuanceEventType.CANCEL_USE,
                IssuanceStatus.USED, IssuanceStatus.ISSUED, AS_OF.minusHours(1));
    }

    /** 어댑터가 지키는 정렬 계약 — (issuance_id, created_at, id) 오름차순 — 을 그대로 흉내 낸다. */
    private static final class FakeHistories implements ReplayHistoryRepository {

        private final Map<Long, List<IssuanceHistoryRecord>> byIssuance = new LinkedHashMap<>();
        private final List<String> requestedWindows = new ArrayList<>();

        private int rangeCalls;

        FakeHistories with(long issuanceId, IssuanceHistoryRecord... histories) {
            byIssuance.put(issuanceId, List.of(histories));
            return this;
        }

        @Override
        public Optional<IssuanceIdRange> issuanceIdRange(LocalDateTime asOf) {
            rangeCalls++;

            return byIssuance.isEmpty()
                    ? Optional.empty()
                    : Optional.of(new IssuanceIdRange(
                            byIssuance.keySet().stream().min(Long::compare).orElseThrow(),
                            byIssuance.keySet().stream().max(Long::compare).orElseThrow()));
        }

        @Override
        public List<IssuanceHistoryRecord> findRange(
                long fromIssuanceId, long toIssuanceId, LocalDateTime asOf) {
            requestedWindows.add(fromIssuanceId + "-" + toIssuanceId);

            return byIssuance.entrySet().stream()
                    .filter(entry -> entry.getKey() >= fromIssuanceId
                            && entry.getKey() <= toIssuanceId)
                    .sorted(Map.Entry.comparingByKey())
                    .flatMap(entry -> entry.getValue().stream()
                            .sorted(Comparator.comparing(IssuanceHistoryRecord::createdAt)
                                    .thenComparingLong(IssuanceHistoryRecord::id)))
                    .toList();
        }
    }
}
