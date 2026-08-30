// 존을 인자로 고정해 변환 자체를 잽니다 — 주변 환경에 안 매인다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>{@code VerifyRunAxisTest} 만으로는 부족하다.</b> 그쪽은 잡을 실제로 돌려 결과를 재는데,
 * 전제가 <i>"테스트 JVM 이 UTC 가 아니다"</i>({@code batch/build.gradle} 의
 * {@code user.timezone=Asia/Seoul}) 라 <b>그 한 줄이 UTC 로 바뀌는 날 통째로 건너뛰어진다</b> —
 * 그러면 {@code onDomainAxis} 를 지워도 빌드가 초록이고, skip 은 리포트에서 눈에 안 띈다.
 * 그 KST 고정의 근거는 {@code TimestampMappingTest} 이고, 그 테스트가 정리되면 함께 정리된다.
 *
 * <p>여기서는 <b>존을 인자로 고정</b>해 변환 자체를 잰다. 주변 환경이 무엇이든 같은 답이다.
 */
class BatchTimeAxisTest {

    /** 초가 0 이 아니게 둔다 — 0 이면 자릿수 문제를 못 본다. */
    private static final LocalDateTime WALL = LocalDateTime.of(2026, 1, 15, 18, 30, 7);

    @Test
    @DisplayName("동쪽 존의 벽시계를 같은 순간의 UTC 벽시계로 옮긴다")
    void movesEastZoneWallClockToUtc() {
        assertThat(BatchTimeAxis.onDomainAxis(WALL, ZoneId.of("Asia/Seoul")))
                .isEqualTo(LocalDateTime.of(2026, 1, 15, 9, 30, 7));
    }

    /**
     * <b>서쪽 존은 반대로 움직인다.</b> 부호를 한 방향으로만 재면 <b>부호를 뒤집은 구현</b>이
     * 살아남는다 — 이 저장소는 시각 축에서 방향을 반대로 적은 적이 두 번 있다.
     */
    @Test
    @DisplayName("서쪽 존은 반대 방향으로 옮긴다")
    void movesWestZoneWallClockToUtc() {
        assertThat(BatchTimeAxis.onDomainAxis(WALL, ZoneId.of("America/New_York")))
                .as("UTC-5 면 UTC 벽시계가 다섯 시간 **뒤**다")
                .isEqualTo(LocalDateTime.of(2026, 1, 15, 23, 30, 7));
    }

    @Test
    @DisplayName("UTC 면 항등이다 — 배포에서는 아무것도 안 한다")
    void isIdentityOnUtc() {
        assertThat(BatchTimeAxis.onDomainAxis(WALL, ZoneOffset.UTC)).isEqualTo(WALL);
        assertThat(BatchTimeAxis.onDomainAxis(WALL, ZoneId.of("Etc/UTC"))).isEqualTo(WALL);
    }

    /**
     * <b>DST 겹침에서는 한 시간 어긋난다 — 한계를 사실로 박아 둔다.</b> 유효 오프셋이 둘인데
     * {@code LocalDateTime} 에는 오프셋이 없어 어느 쪽이었는지 알 수 없고, {@code atZone} 은
     * <b>이른 쪽</b>을 고른다. 그런 존은 {@code DefaultZoneGuard} 가 고정 오프셋만
     * 통과시켜 <b>기동을 거절</b>하므로 배포에서는 도달하지 않는다.
     */
    @Test
    @DisplayName("DST 겹침 시각은 이른 오프셋을 고른다 — 그 존은 가드가 거절한다")
    void picksEarlierOffsetOnOverlap() {
        ZoneId london = ZoneId.of("Europe/London");
        LocalDateTime overlap = LocalDateTime.of(2026, 10, 25, 1, 30);

        assertThat(london.getRules().getValidOffsets(overlap))
                .as("겹침 구간이라 유효 오프셋이 둘이다 — 전제")
                .hasSize(2);
        assertThat(BatchTimeAxis.onDomainAxis(overlap, london))
                .as("이른 쪽(+01:00)을 골라 한 시간 이르게 나온다")
                .isEqualTo(LocalDateTime.of(2026, 10, 25, 0, 30));
        assertThat(DefaultZoneGuard.isUtc(london))
                .as("그래서 이 존은 기동에서 거절된다 — 배포에서는 도달하지 않는다")
                .isFalse();
    }

    /**
     * <b>갭(봄 전이)은 겹침보다 더 크게 어긋나지만 도달할 수 없다.</b> 유효 오프셋이
     * <b>비어</b> {@code atZone} 이 로컬 시각 자체를 한 시간 밀어 버린다. 다만 배치 메타는
     * 그 값을 <b>만들 수 없다</b> — 벽시계가 그 구간을 지나지 않으므로
     * {@code LocalDateTime.now()} 가 갭 안의 값을 낼 수 없다. 한계를 사실로 남긴다.
     */
    @Test
    @DisplayName("갭은 벽시계가 그 구간을 안 지나 배치 메타가 만들 수 없다")
    void gapIsUnreachableFromBatchMeta() {
        ZoneId london = ZoneId.of("Europe/London");
        LocalDateTime gap = LocalDateTime.of(2026, 3, 29, 1, 30);

        assertThat(london.getRules().getValidOffsets(gap))
                .as("갭이라 유효 오프셋이 없다 — 벽시계가 이 값을 안 지난다")
                .isEmpty();
        assertThat(BatchTimeAxis.onDomainAxis(gap, london))
                .as("그래도 부르면 atZone 이 로컬 시각을 한 시간 민다")
                .isEqualTo(LocalDateTime.of(2026, 3, 29, 1, 30));
    }

    @Test
    @DisplayName("null 은 안 받는다 — 조용히 흘려 보내면 실패가 저장 계층으로 밀린다")
    void rejectsNull() {
        assertThatThrownBy(() -> BatchTimeAxis.onDomainAxis(null, ZoneOffset.UTC))
                .isInstanceOf(NullPointerException.class);
    }
}
