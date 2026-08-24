// 되읽기 창(7일)과 보존 최솟값이 같은 축이라는 것을 소스에서 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>두 되읽기의 창은 SQL 안에 리터럴로 있고, 그것을 지키는 가드는 다른 클래스의 상수다.</b>
 * 둘을 잇는 것이 코드에도 테스트에도 없어서, 한쪽만 고쳐도 전부 초록이었다.
 *
 * <p><b>왜 이 조합이 위험한가.</b> 가드는 {@code metadata-keep-days} 가
 * {@link CleanupJobConfig#REFRESH_WINDOW_DAYS} 보다 크다는 것만 본다. 누가 되읽기의 창만
 * 14일로 넓히면, {@code metadata-keep-days=8} 배포에서 <b>창 안(9~14일 전)의 마지막 성공이
 * 보존 삭제로 지워진다.</b> 게이지가 {@code NaN} 이 되고 {@code CleanupNeverSucceeded}
 * (critical)가 영구 발화하는데, 기동 가드는 상수가 7 그대로라 통과한다.
 *
 * <p><b>{@code config} 패키지가 아니라 여기 둔다.</b> 재는 대상이 그 상수이고 그것은
 * 패키지 전용이다 — 접근을 넓히는 것보다 테스트를 옮기는 편이 계약을 안 늘린다.
 *
 * <p><b>소스를 읽어서 잰다.</b> 그 창은 SQL 문자열 안이라 실행으로는 값을 못 꺼낸다 —
 * {@code NoWallClockInBatchTest} 가 같은 이유로 같은 방식을 쓴다.
 */
class RefreshWindowLockTest {

    private static final Path SOURCE_ROOT = Path.of("src/main/java/com/kafkick/batch/config");

    /** {@code AND e.END_TIME > DATE_SUB(NOW(), INTERVAL 7 DAY)} 의 숫자를 뽑는다. */
    private static final Pattern WINDOW =
            Pattern.compile("DATE_SUB\\(NOW\\(\\), INTERVAL (\\d+) DAY\\)");

    @Test
    @DisplayName("두 되읽기의 7일 창이 보존 하한이 지키는 그 창과 같은 값이다")
    void refreshWindowMatchesTheRetentionFloor() throws IOException {
        assertWindowIs("BatchRunMetricsRefresher.java");
        assertWindowIs("ExpirePendingRefresher.java");
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
                .isEqualTo(CleanupJobConfig.REFRESH_WINDOW_DAYS + 1);
    }

    private static void assertWindowIs(String fileName) throws IOException {
        String source = Files.readString(SOURCE_ROOT.resolve(fileName), StandardCharsets.UTF_8);
        // **첫 매치만 보면 안 된다.** 되읽기가 하나 더 붙는 것은 예정된 일이고
        // (ExpirePendingRefresher 가 이미 둘째다), 그때 새 조회가 다른 창을 쓰면
        // find() 하나로는 조용히 통과한다.
        List<Integer> windows = WINDOW.matcher(source).results()
                .map(result -> Integer.parseInt(result.group(1)))
                .toList();

        assertThat(windows)
                .as("%s 에서 되읽기 창을 못 찾았다 — 조회를 바꿨다면 이 테스트도 함께 고쳐라. "
                        + "그냥 지우면 창과 보존 최솟값을 잇는 것이 다시 사라진다", fileName)
                .isNotEmpty();
        assertThat(windows)
                .as("%s 의 창을 넓히면 CleanupJobConfig.REFRESH_WINDOW_DAYS 도 함께 올려야 "
                        + "한다. 안 그러면 창 안의 마지막 성공이 보존 삭제로 지워진다", fileName)
                .containsOnly(CleanupJobConfig.REFRESH_WINDOW_DAYS);
    }
}
