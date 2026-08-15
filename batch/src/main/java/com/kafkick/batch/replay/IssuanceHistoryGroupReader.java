// 발급건 식별자 구간으로 이력을 읽어 발급건 단위로 내보냅니다. Step 0 의 입력원입니다.
package com.kafkick.batch.replay;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStreamReader;

import com.kafkick.core.verification.replay.IssuanceHistoryRecord;
import com.kafkick.core.verification.replay.IssuanceIdRange;
import com.kafkick.core.verification.replay.ReplayHistoryRepository;

/**
 * 창 하나를 통째로 읽어 발급건 단위로 쪼개 내보냅니다. 창은 <b>발급건 식별자</b> 범위라
 * 한 발급건의 이력이 두 창에 걸치는 일이 없습니다.
 *
 * <p><b>재시작 위치는 창 단위입니다.</b> 버퍼에 아직 안 나간 묶음이 남아 있으면 그 묶음이
 * 나온 창의 시작을 저장합니다. 다음 창을 저장하면 남은 묶음이 통째로 사라지고, 그 발급건들은
 * {@code asof_state} 에 아예 안 생깁니다.
 *
 * <p>그래서 재시작하면 창 하나를 다시 읽어 이미 쓴 묶음을 다시 내보냅니다. 라이터가 UPSERT 라
 * 같은 값이 다시 써질 뿐 결과는 같습니다. 손실보다 중복이 낫습니다.
 */
public class IssuanceHistoryGroupReader implements ItemStreamReader<IssuanceHistoryGroup> {

    private static final String KEY_RANGE_MIN = "replay.range.min";
    private static final String KEY_RANGE_MAX = "replay.range.max";
    private static final String KEY_WINDOW_START = "replay.window.start";

    private final ReplayHistoryRepository repository;
    private final LocalDateTime asOf;
    private final long windowSize;

    private final Deque<IssuanceHistoryGroup> buffer = new ArrayDeque<>();

    /** 훑을 것이 없으면 null 로 남습니다. */
    private IssuanceIdRange range;

    private long nextWindowStart;
    private long drainingWindowStart;
    private boolean exhausted;

    public IssuanceHistoryGroupReader(
            ReplayHistoryRepository repository,
            LocalDateTime asOf,
            long windowSize
    ) {
        if (windowSize < 1) {
            throw new IllegalArgumentException("창 크기는 1 이상이어야 합니다. 값=" + windowSize);
        }

        this.repository = repository;
        this.asOf = asOf;
        this.windowSize = windowSize;
    }

    /**
     * 훑을 범위는 처음 열 때 한 번만 구해 실행 컨텍스트에 박아 둔다.
     * 재시작할 때 다시 구하면 그 사이 들어온 이력 때문에 범위가 달라져 결정론이 깨진다.
     */
    @Override
    public void open(ExecutionContext executionContext) {
        buffer.clear();
        exhausted = false;

        if (executionContext.containsKey(KEY_RANGE_MIN)) {
            range = new IssuanceIdRange(
                    executionContext.getLong(KEY_RANGE_MIN),
                    executionContext.getLong(KEY_RANGE_MAX));
            nextWindowStart = executionContext.getLong(KEY_WINDOW_START);
        } else {
            Optional<IssuanceIdRange> found = repository.issuanceIdRange(asOf);
            range = found.orElse(null);
            nextWindowStart = found.map(IssuanceIdRange::min).orElse(0L);

            found.ifPresent(bounds -> {
                executionContext.putLong(KEY_RANGE_MIN, bounds.min());
                executionContext.putLong(KEY_RANGE_MAX, bounds.max());
                executionContext.putLong(KEY_WINDOW_START, bounds.min());
            });
        }

        drainingWindowStart = nextWindowStart;
    }

    @Override
    public IssuanceHistoryGroup read() {
        while (buffer.isEmpty()) {
            if (range == null || exhausted || nextWindowStart > range.max()) {
                return null;
            }
            loadNextWindow();
        }

        return buffer.poll();
    }

    /**
     * 버퍼가 비어 있어야만 다음 창으로 넘어간 것으로 본다.
     * 남아 있으면 그 묶음들이 나온 창을 다시 읽어야 빠뜨리지 않는다.
     */
    @Override
    public void update(ExecutionContext executionContext) {
        if (range == null) {
            return;
        }

        executionContext.putLong(
                KEY_WINDOW_START, buffer.isEmpty() ? nextWindowStart : drainingWindowStart);
    }

    @Override
    public void close() {
        buffer.clear();
    }

    private void loadNextWindow() {
        long windowEnd = windowEndFrom(nextWindowStart);
        List<IssuanceHistoryRecord> rows =
                repository.findRange(nextWindowStart, windowEnd, asOf);

        drainingWindowStart = nextWindowStart;
        if (windowEnd == Long.MAX_VALUE) {
            // 더 밀 자리가 없다. +1 하면 음수로 돌아 "끝났다" 판정이 뒤집히고 처음부터 다시 읽는다.
            exhausted = true;
        } else {
            nextWindowStart = windowEnd + 1;
        }
        buffer.addAll(groupByIssuance(rows));
    }

    /** {@code start + windowSize - 1} 을 그대로 더하면 넘칠 수 있어 남은 폭을 먼저 잰다. */
    private long windowEndFrom(long start) {
        long remaining = range.max() - start;
        return remaining < windowSize - 1 ? range.max() : start + windowSize - 1;
    }

    /**
     * 이미 {@code issuance_id} 오름차순으로 정렬돼 온 것을 전제로 이어진 구간을 자른다.
     * 정렬이 깨지면 같은 발급건이 두 묶음으로 나뉘어 뒤 묶음이 앞 묶음을 덮어쓴다.
     */
    private static List<IssuanceHistoryGroup> groupByIssuance(List<IssuanceHistoryRecord> rows) {
        List<IssuanceHistoryGroup> groups = new ArrayList<>();
        List<IssuanceHistoryRecord> current = new ArrayList<>();

        for (IssuanceHistoryRecord row : rows) {
            if (!current.isEmpty() && row.issuanceId() != current.get(0).issuanceId()) {
                groups.add(new IssuanceHistoryGroup(current.get(0).issuanceId(), current));
                current = new ArrayList<>();
            }
            current.add(row);
        }

        if (!current.isEmpty()) {
            groups.add(new IssuanceHistoryGroup(current.get(0).issuanceId(), current));
        }

        return groups;
    }
}
