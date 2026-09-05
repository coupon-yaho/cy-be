package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.kafkick.core.notification.NotificationSender;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationStatus;

/**
 * <b>{@code null} 을 거절하는 것은 구현 취향이 아니라 포트의 계약이다.</b>
 * ({@code NotificationSender#send} javadoc)
 *
 * <p>키가 없으면 받는 쪽이 중복을 <b>못 합친다.</b> 안 보낸 것은 재시도로 회복되지만
 * <b>키 없이 두 번 간 것은 회복이 없다</b> — 그래서 빈 키로 조용히 보내느니 그 자리에서
 * 멈춘다. 계약을 javadoc 에만 적어 두면 다음 구현이 그냥 빠뜨린다.
 *
 * <p>⚠️ 실제로 빠뜨려 있었다. {@link MockNotificationSender} 가 {@code notification} 만 보고
 * 키는 안 봤는데, <b>이 저장소의 통합 테스트가 그것으로 돈다.</b> 키를 빠뜨린 호출자가
 * 로컬에서 전부 통과하고 실제 연동에서만 터졌을 것이다.
 */
class NotificationSenderNullContractTest {

    private static Notification notification() {
        return new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.PENDING, 2, 0, null, "member:20", "메시지",
                Instant.parse("2026-09-05T00:00:00Z"), Instant.parse("2026-09-05T00:00:00Z"),
                null, null);
    }

    static Stream<NotificationSender> senders() {
        return Stream.of(
                new MockNotificationSender(),
                new HttpNotificationSender("http://notify.test/send",
                        Duration.ofMillis(30), Duration.ofMillis(60)));
    }

    @ParameterizedTest
    @MethodSource("senders")
    @DisplayName("어떤 발송기든 키가 없으면 보내지 않고 멈춘다")
    void everySenderRefusesAMissingIdempotencyKey(NotificationSender sender) {
        assertThatThrownBy(() -> sender.send(notification(), null))
                .as("빈 키로 보내면 받는 쪽이 중복을 못 합친다 — 회복이 없는 실패다")
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @MethodSource("senders")
    @DisplayName("어떤 발송기든 알림이 없으면 보내지 않고 멈춘다")
    void everySenderRefusesAMissingNotification(NotificationSender sender) {
        assertThatThrownBy(() -> sender.send(null, "41:1"))
                .isInstanceOf(NullPointerException.class);
    }

    /**
     * <b>위 두 검사는 여기 적힌 구현만 본다.</b> 새 발송기를 만들고 이 목록에 안 넣으면
     * 계약이 그 구현만 비껴간다 — 그것이 정확히 지금 고친 구멍의 모양이다. 그래서 소스에서
     * 구현을 세어 목록과 맞춘다: 새 발송기를 만드는 사람이 <b>여기서 걸려</b> 추가하게 된다.
     */
    @Test
    @DisplayName("소스의 발송기 구현이 늘면 이 계약 목록도 늘어야 한다")
    void theContractListCoversEveryImplementationInTheSource() throws IOException {
        Path repo = Path.of("..", "..").toAbsolutePath().normalize();
        List<String> implementations;
        try (Stream<Path> files = Files.walk(repo)) {
            implementations = files
                    .filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> path.toString().contains("/src/main/java/"))
                    .filter(NotificationSenderNullContractTest::implementsTheSenderPort)
                    .map(path -> path.getFileName().toString().replace(".java", ""))
                    .sorted()
                    .toList();
        }

        assertThat(implementations)
                .as("새 발송기가 생겼다면 senders() 에 추가하고 이 기대값도 늘려라")
                .containsExactly("HttpNotificationSender", "MockNotificationSender");
    }

    private static boolean implementsTheSenderPort(Path path) {
        try {
            return Files.readString(path).contains("implements NotificationSender");
        } catch (IOException unreadable) {
            throw new IllegalStateException(path.toString(), unreadable);
        }
    }
}
