package com.kafkick.core.observation.attempt;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;

import tools.jackson.databind.ObjectMapper;

class AttemptLiveEntryTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class));

    /**
     * 이 티켓의 인수 조건 — <b>PII 금지.</b> 화면에도, <b>이벤트에도</b> 나오면 안 된다.
     *
     * <p>관리자 응답이 아니라 <b>직렬화된 본문</b>을 본다. 응답 DTO 만 검사하면 Redis 에는
     * 원문이 이미 앉은 뒤다 — 화면에서만 빠질 뿐이고, Redis 를 직접 열어 보면 그대로 보인다.
     * 이 저장소가 Redis 를 사람 손으로 들여다보는 것을 막는 장치는 없다.
     *
     * <p>이름·이메일·전화·JWT·Entry-Token 은 애초에 {@link IssuanceFlowEvent} 에 없다. 그
     * 사실에 기대는 대신, 이벤트가 실제로 싣는 값 중 나가면 안 되는 것들을 직접 확인한다.
     */
    @Test
    void writesNoRequestTracingOrProducerIdentityIntoTheBuffer() {
        contextRunner.run(context -> {
            ObjectMapper objectMapper = context.getBean("jacksonJsonMapper", ObjectMapper.class);
            String json = objectMapper.writeValueAsString(AttemptLiveEntry.from(record(issued())));

            assertThat(json)
                    .doesNotContain("requestId").doesNotContain("request-secret-1")
                    .doesNotContain("producerInstanceId").doesNotContain("api-instance-7")
                    .doesNotContain("benchmarkRunId")
                    .doesNotContain("engineVersion").doesNotContain("releaseStage")
                    .doesNotContain("queueMode").doesNotContain("dependency");
        });
    }

    /** 쿠폰 코드는 앞 8자만 나간다. 전문이 나가면 그 코드로 무엇이든 할 수 있다. */
    @Test
    void carriesOnlyTheFirstEightCharactersOfTheCouponCode() {
        AttemptLiveEntry entry = AttemptLiveEntry.from(record(issued()));

        assertThat(entry.issuanceCodeMasked()).isEqualTo("ABCD1234");
        assertThat("ABCD1234WXYZ5678").startsWith(entry.issuanceCodeMasked());
        assertThat(entry.issuanceCodeMasked()).hasSize(AttemptLiveEntry.CODE_PREFIX_LENGTH);
    }

    /**
     * 8 자보다 짧은 코드에서 터지지 않는다.
     *
     * <p>계약은 {@code requireText(issuanceCode, 16, ...)} 이라 더 짧은 값도 유효하다.
     * {@code substring} 을 조건 없이 부르면 {@code StringIndexOutOfBoundsException} 이고,
     * 그 예외는 발급이 아니라 화면을 죽인다 — 컨슈머가 삼켜도 그 건은 화면에서 사라진다.
     */
    @Test
    void leavesShortCodesAlone() {
        IssuanceFlowEvent shortCode = new IssuanceFlowEventFactory(UUID::randomUUID)
                .issued(context(), 301L, "ABC");

        assertThat(AttemptLiveEntry.from(record(shortCode)).issuanceCodeMasked()).isEqualTo("ABC");
    }

    /** 코드가 없는 이벤트는 null 이다. 빈 문자열로 바뀌면 화면이 "코드가 있다" 로 읽는다. */
    @Test
    void keepsAbsentCodesAbsent() {
        IssuanceFlowEvent attempt = new IssuanceFlowEventFactory(UUID::randomUUID)
                .issueAttempt(context());

        assertThat(AttemptLiveEntry.from(record(attempt)).issuanceCodeMasked()).isNull();
    }

    /**
     * 화이트리스트가 <b>실제로 화이트리스트인지</b> 본다.
     *
     * <p>{@link IssuanceFlowEvent} 에 필드가 하나 늘면 이 테스트가 깨진다. 그때 옮길지 뺄지를
     * 정하는 것이 요점이다 — 복사 후 제거로 만들면 새 필드가 <b>자동으로</b> 화면과 Redis 로
     * 나가고, 그 결정을 아무도 하지 않은 채 나간다.
     */
    @Test
    void countsTheEventFieldsSoNewOnesForceADecision() {
        assertThat(componentNames(IssuanceFlowEvent.class)).hasSize(21);
        assertThat(componentNames(AttemptLiveEntry.class)).hasSize(14);
        assertThat(componentNames(AttemptLiveEntry.class))
                .as("정제본에 없어야 하는 이름")
                .doesNotContain("requestId", "producerInstanceId", "benchmarkRunId",
                        "engineVersion", "releaseStage", "queueMode", "dependency",
                        "schemaVersion", "issuanceCode");
    }

    /** {@code ingestedAt} 은 컨슈머 시각이지 프로듀서 시각이 아니다. */
    @Test
    void takesIngestedAtFromTheRecordNotTheEvent() {
        AttemptLiveEntry entry = AttemptLiveEntry.from(record(issued()));

        assertThat(entry.occurredAt()).isEqualTo(Instant.parse("2026-08-25T00:00:00Z"));
        assertThat(entry.ingestedAt()).isEqualTo(Instant.parse("2026-08-25T00:00:02Z"));
    }

    private static AttemptRecord record(IssuanceFlowEvent event) {
        return new AttemptRecord(event, "coupon.issue.attempt", 0, 1L,
                Instant.parse("2026-08-25T00:00:02Z"));
    }

    private static IssuanceFlowEvent issued() {
        return new IssuanceFlowEventFactory(UUID::randomUUID)
                .issued(context(), 301L, "ABCD1234WXYZ5678");
    }

    private static IssuanceFlowEvent.Ctx context() {
        return new IssuanceFlowEvent.Ctx("request-secret-1", 101L, 201L, Grade.GOLD, false,
                Instant.parse("2026-08-25T00:00:00Z"), EngineVersion.V3, ReleaseStage.V3,
                QueueMode.ADAPTIVE, 901L, "api-instance-7");
    }

    private static List<String> componentNames(Class<?> recordType) {
        return Arrays.stream(recordType.getRecordComponents())
                .map(RecordComponent::getName).toList();
    }
}
