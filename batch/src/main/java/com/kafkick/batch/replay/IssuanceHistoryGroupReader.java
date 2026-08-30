// 발급건 식별자 구간으로 이력을 읽어 발급건 단위로 내보냅니다. Step 0 의 입력원입니다.
package com.kafkick.batch.replay;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStreamReader;

import com.kafkick.core.verification.replay.IssuanceHistoryRecord;
import com.kafkick.core.verification.replay.ReplayHistoryRepository;
import com.kafkick.core.verification.replay.ReplayScanRange;

/**
 * 창 하나를 통째로 읽어 발급건 단위로 쪼개 내보냅니다. 창은 <b>발급건 식별자</b> 범위라
 * 한 발급건의 이력이 두 창에 걸치는 일이 없습니다.
 *
 * <p><b>훑을 경계를 스스로 재지 않습니다.</b> 실행 시작 Step 이 재서 넘겨준 것을 씁니다.
 * 리더가 열리는 시점은 실행 행이 만들어진 뒤라, 여기서 재면 그 사이 생긴 발급건이
 * 경계 밖으로 밀려 영원히 안 읽힙니다.
 *
 * <p><b>지금 재시작 경로는 돌지 않습니다.</b> {@code VerifyJobConfig#verifyJob} 이
 * {@code preventRestart()} 라 실패한 실행을 이어 돌릴 수 없고, 다시 돌리려면 {@code attempt} 를
 * 올려 새 실행으로 갑니다. 아래 창 체크포인트는 그 결정을 되돌릴 때를 위해 남겨 둔 것이고,
 * <b>한 번도 실행된 적이 없다는 것을 먼저 확인해야 합니다.</b>
 *
 * <p>체크포인트 규칙은 이렇습니다 — 버퍼에 아직 안 나간 묶음이 남아 있으면 그 묶음이 나온 창의
 * 시작을 저장합니다. 다음 창을 저장하면 남은 묶음이 통째로 사라지고, 그 발급건들은
 * {@code asof_state} 에 아예 안 생깁니다. 그래서 창 하나를 다시 읽어 중복으로 내보냅니다 —
 * 라이터가 UPSERT 라 같은 값이 다시 써질 뿐입니다. 손실보다 중복이 낫습니다.
 */
public class IssuanceHistoryGroupReader implements ItemStreamReader<IssuanceHistoryGroup> {

    /**
     * 창 하나가 통째로 힙에 올라옵니다. 발급건당 이력이 평균 1.8행이므로
     * 20만 창이면 이력 약 36만 행이 동시에 상주합니다. 청크 크기를 줄여도 이건 안 줄어듭니다.
     */
    static final long MAX_WINDOW_SIZE = 200_000L;

    private static final String KEY_WINDOW_START = "replay.window.start";

    private final ReplayHistoryRepository repository;
    private final LocalDateTime asOf;
    private final ReplayScanRange scanRange;
    private final long windowSize;

    private final Deque<IssuanceHistoryGroup> buffer = new ArrayDeque<>();

    private long nextWindowStart;
    private long drainingWindowStart;
    private boolean exhausted;

    /**
     * @param scanRange 실행 시작 Step 이 얼린 경계. 접을 이력이 없으면 null 이고,
     *                  그때는 아무것도 내보내지 않는다
     */
    public IssuanceHistoryGroupReader(
            ReplayHistoryRepository repository,
            LocalDateTime asOf,
            ReplayScanRange scanRange,
            long windowSize
    ) {
        if (windowSize < 1 || windowSize > MAX_WINDOW_SIZE) {
            throw new IllegalArgumentException(
                    "창 크기는 1 이상 " + MAX_WINDOW_SIZE + " 이하여야 합니다. 값=" + windowSize);
        }

        this.repository = repository;
        this.asOf = asOf;
        this.scanRange = scanRange;
        this.windowSize = windowSize;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        buffer.clear();
        exhausted = false;

        if (scanRange == null) {
            return;
        }

        nextWindowStart = executionContext.containsKey(KEY_WINDOW_START)
                ? executionContext.getLong(KEY_WINDOW_START)
                : scanRange.minIssuanceId();
        drainingWindowStart = nextWindowStart;
    }

    @Override
    public IssuanceHistoryGroup read() {
        while (buffer.isEmpty()) {
            if (scanRange == null || exhausted || nextWindowStart > scanRange.maxIssuanceId()) {
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
        if (scanRange == null) {
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
        List<IssuanceHistoryRecord> rows = repository.findRange(
                nextWindowStart, windowEnd, asOf, scanRange.maxHistoryId());

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
        long remaining = scanRange.maxIssuanceId() - start;
        return remaining < windowSize - 1 ? scanRange.maxIssuanceId() : start + windowSize - 1;
    }

    /**
     * 이미 {@code issuance_id} 오름차순으로 정렬돼 온 것을 전제로 이어진 구간을 자른다.
     *
     * <p><b>전제가 깨지면 즉시 끊는다.</b> 같은 발급건이 두 묶음으로 갈리면 뒤 묶음이
     * 중간 상태로 {@code asof_state} 를 덮어쓰는데, 라이터가 UPSERT 라 아무 소리도 안 난다.
     * 어댑터의 {@code ORDER BY} 가 사라지는 회귀를 여기서 드러낸다.
     */
    private static List<IssuanceHistoryGroup> groupByIssuance(List<IssuanceHistoryRecord> rows) {
        List<IssuanceHistoryGroup> groups = new ArrayList<>();
        List<IssuanceHistoryRecord> current = new ArrayList<>();

        for (IssuanceHistoryRecord row : rows) {
            if (!current.isEmpty() && row.issuanceId() != current.get(0).issuanceId()) {
                if (row.issuanceId() < current.get(0).issuanceId()) {
                    throw new IllegalStateException(
                            "이력이 발급건 오름차순이 아닙니다. 앞=" + current.get(0).issuanceId()
                                    + " 뒤=" + row.issuanceId());
                }
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
