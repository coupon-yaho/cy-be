package com.kafkick.api.observation.issuance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class CompositeEventRecorderTest {

    @Test
    void rejectsNullDelegateListsAndDelegates() {
        assertThatThrownBy(() -> new CompositeEventRecorder((EventRecorder[]) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("delegates");
        assertThatThrownBy(() -> new CompositeEventRecorder((EventRecorder) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("delegate");
    }

    @Test
    void rejectsAnEmptyDelegateList() {
        assertThatThrownBy(CompositeEventRecorder::new)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("delegates must not be empty");
    }

    @Test
    void isolatesMultipleFailuresAndDeliversToHealthyDelegates(CapturedOutput output) {
        AtomicInteger delivered = new AtomicInteger();
        // 같은 인스턴스를 두 번 넘기면 합성기가 하나로 합친다. 실패 sink 가 둘인 상황을 재려면
        // 서로 다른 인스턴스여야 한다.
        EventRecorder failing = ignored -> { throw new IllegalStateException("unavailable"); };
        EventRecorder alsoFailing = ignored -> { throw new IllegalStateException("unavailable"); };

        CompositeEventRecorder recorder = new CompositeEventRecorder(
                failing, alsoFailing, ignored -> delivered.incrementAndGet());
        recorder.record(event());
        recorder.record(event());

        assertThat(delivered).hasValue(2);
        assertThat(output).contains("발급 관측 전달에 실패했습니다").contains("recorder=");
        assertThat(occurrences(output.getAll(), "발급 관측 전달에 실패했습니다")).isEqualTo(1);
    }

    @Test
    void appliesTheConfiguredFailureLogInterval(CapturedOutput output) {
        EventRecorder failing = ignored -> { throw new IllegalStateException("unavailable"); };

        CompositeEventRecorder recorder = new CompositeEventRecorder(Duration.ofNanos(1), failing);
        recorder.record(event());
        recorder.record(event());

        assertThat(occurrences(output.getAll(), "발급 관측 전달에 실패했습니다")).isEqualTo(2);
    }

    @Test
    void rejectsNonPositiveFailureLogIntervals() {
        EventRecorder delegate = ignored -> { };

        assertThatThrownBy(() -> new CompositeEventRecorder(Duration.ZERO, delegate))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("failureLogInterval must be positive");
    }

    @Test
    void deliversOnceToADelegateThatAlsoAppearsInsideANestedComposite() {
        AtomicInteger leafCalls = new AtomicInteger();
        EventRecorder leaf = ignored -> leafCalls.incrementAndGet();

        CompositeEventRecorder recorder = new CompositeEventRecorder(
                leaf, new CompositeEventRecorder(leaf));
        recorder.record(event());

        assertThat(leafCalls).hasValue(1);
    }

    private static IssuanceFlowEvent event() {
        return new IssuanceFlowEventFactory(java.util.UUID::randomUUID).issueAttempt(
                new IssuanceFlowEvent.Ctx(
                        "composite", 101L, 201L, Grade.GOLD, false,
                        Instant.parse("2026-08-23T00:00:00Z"), EngineVersion.V3, ReleaseStage.V3,
                        QueueMode.ADAPTIVE, 901L, "api-1"
                )
        );
    }

    private static int occurrences(String text, String needle) {
        return text.split(java.util.regex.Pattern.quote(needle), -1).length - 1;
    }
}
