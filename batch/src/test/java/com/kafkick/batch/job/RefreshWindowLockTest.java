// 되읽기 창과 보존 최솟값이 한 상수에서 나온다는 것을 소스에서 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.batch.config.BatchMetadataWindow;

/**
 * <b>한때 같은 7이 세 곳에 따로 박혀 있었다</b> — 두 되읽기의 {@code INTERVAL 7 DAY} 리터럴과
 * 보존 하한이 쓰는 상수. 셋을 잇는 것이 코드에 없어 한쪽만 고쳐도 전부 초록이었고, 이
 * 클래스가 <b>소스를 정규식으로 읽어</b> 그 간극을 메우고 있었다.
 *
 * <p><b>CY-470 이 그 간극을 코드에서 없앴다.</b> 두 SQL 이
 * {@link BatchMetadataWindow#LOOKBACK_DAYS} 를 {@code .formatted} 로 끼워 넣으므로
 * 어긋날 자리가 없다. 그래서 이 테스트가 재는 것도 바뀐다 — <b>값이 같은가</b> 가 아니라
 * <b>리터럴이 다시 들어오지 않았는가</b> 다.
 *
 * <p><b>왜 아직 필요한가.</b> 되읽기가 하나 더 붙는 것은 예정된 일이고
 * ({@code ExpirePendingRefresher} 가 이미 둘째다), 새 조회를 쓰는 사람이 앞의 둘을 안 보고
 * {@code INTERVAL 7 DAY} 라고 적으면 <b>그 순간 세 곳 시절로 돌아간다.</b> 그때 무엇이
 * 깨지는지는 여전히 같다 — 누가 창만 14일로 넓히면 {@code metadata-keep-days=8} 배포에서
 * <b>창 안(9~14일 전)의 마지막 성공이 보존 삭제로 지워지고</b>, 게이지가 {@code NaN} 이 되어
 * {@code CleanupNeverSucceeded}(critical)가 영구 발화한다.
 *
 * <p><b>소스를 읽어서 잰다.</b> 그 창은 SQL 문자열 안이라 실행으로는 값을 못 꺼낸다 —
 * {@code NoWallClockInBatchTest} 가 같은 이유로 같은 방식을 쓴다.
 *
 * <p><b>{@code config} 패키지가 아니라 여기 둔다.</b> 아래 둘째 테스트가 재는
 * {@code MIN_METADATA_KEEP_DAYS} 가 {@code batch.job} 패키지 전용이다 — 접근을 넓히는 것보다
 * 테스트를 여기 두는 편이 계약을 안 늘린다.
 */
class RefreshWindowLockTest {

    private static final Path SOURCE_ROOT = Path.of("src/main/java/com/kafkick/batch/config");

    /** 되읽기 둘. 창을 쓰는 조회가 여기에만 있다. */
    private static final java.util.List<String> REFRESHERS =
            java.util.List.of("BatchRunMetricsRefresher.java", "ExpirePendingRefresher.java");

    /** {@code AND e.END_TIME > DATE_SUB(NOW(), INTERVAL 7 DAY)} 처럼 <b>숫자를 박은</b> 형태. */
    private static final Pattern HARDCODED_WINDOW =
            Pattern.compile("DATE_SUB\\(NOW\\(\\), INTERVAL \\d+ DAY\\)");

    /** 상수를 끼워 넣는 정상 형태. */
    private static final Pattern DERIVED_WINDOW =
            Pattern.compile("DATE_SUB\\(NOW\\(\\), INTERVAL %d DAY\\)");

    @Test
    @DisplayName("되읽기의 창이 리터럴이 아니라 상수에서 나온다")
    void refreshWindowComesFromTheSharedConstant() throws IOException {
        for (String fileName : REFRESHERS) {
            String source = Files.readString(
                    SOURCE_ROOT.resolve(fileName), StandardCharsets.UTF_8);

            assertThat(DERIVED_WINDOW.matcher(source).results().count())
                    .as("%s 에서 창 조회를 못 찾았다 — 조회를 바꿨다면 이 테스트도 함께 고쳐라. "
                            + "그냥 지우면 창과 보존 최솟값을 잇는 것이 다시 사라진다", fileName)
                    .isGreaterThan(0);
            assertThat(HARDCODED_WINDOW.matcher(source).results().count())
                    .as("%s 에 숫자를 박은 창이 있다. BatchMetadataWindow.LOOKBACK_DAYS 를 "
                            + "%%d 로 끼워 넣어라 — 리터럴은 보존 하한과 따로 움직이고, "
                            + "그 사실은 잡이 보존 기간 넘게 연속 실패한 날에야 드러난다", fileName)
                    .isZero();
            assertThat(source)
                    .as("%s 가 창을 어디서 받는지 이름으로 남아 있어야 한다", fileName)
                    .contains("BatchMetadataWindow.LOOKBACK_DAYS");
        }
    }

    /**
     * <b>이건 선언을 다시 쓴 항등식이라 그 자체로는 못 깨진다.</b> 여기 두는 이유는 <i>왜</i>
     * 하나 더 큰지를 한 자리에 남기기 위해서다 — 실제 가드는
     * {@code CleanupJobSettingsTest#rejectKeepDaysInsideRefreshWindow} 가 리터럴 7/8 로 재고,
     * 상수를 바꾸면 그쪽이 빨개진다.
     */
    @Test
    @DisplayName("보존 최솟값은 창보다 하루 크다 — 같으면 삭제가 창 안을 건드릴 수 있다")
    void retentionFloorIsStrictlyGreaterThanTheWindow() {
        assertThat(CleanupJobConfig.MIN_METADATA_KEEP_DAYS)
                .as("검사가 < MIN 이라 이 값 자체는 통과한다. 창과 같으면 보존과 창이 겹쳐 "
                        + "잡이 하루만 실패해도 마지막 성공이 컷오프 위에 놓인다")
                .isEqualTo(BatchMetadataWindow.LOOKBACK_DAYS + 1);
    }
}
