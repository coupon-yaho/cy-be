package com.kafkick.infra.mq.attempt;

import java.util.Objects;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.infra.mq.config.KafkaTopicConfig;
import com.kafkick.infra.mq.config.PartitionKeys;

/**
 * 발급 경로가 관측 이벤트를 내보내는 유일한 지점. <b>여기서 실패를 끝낸다.</b>
 *
 * <h2>실패가 오는 길이 두 갈래다 — 그래서 세는 자리는 하나여야 한다</h2>
 *
 * 실측으로 확인한 순서는 이렇다.
 *
 * <ol>
 *   <li>{@code KafkaProducer.send()} 는 메타데이터 미확보·버퍼 만석에서 <b>던지지 않고</b>
 *       이미 실패한 future 를 돌려주며 콜백을 호출 스레드에서 인라인 실행한다.</li>
 *   <li>그런데 {@code KafkaTemplate} 은 그 future 가 {@code isDone()} 이면 그 자리에서
 *       {@code get()} 을 호출해 원인을 {@code KafkaException("Send failed")} 로 감싸
 *       <b>다시 던진다.</b></li>
 * </ol>
 *
 * <p>처음에는 {@code ProducerListener} 로도 세고 {@code catch} 로도 셌다. 그 결과 <b>발행 1회에
 * 카운터가 2.0</b> 이 됐고 원인도 {@code timeout} 1 + {@code other} 1 로 갈렸다(직접 돌려서
 * 확인했다). 실패율 경보를 발급 TPS 대비로 잡으면 100% 를 넘는 값이 나온다.
 *
 * <p>그래서 리스너를 걷어내고 {@code whenComplete} 하나로 모았다. 두 경로가 <b>배타</b>가 된다 —
 * 동기로 다시 던져지면 {@code whenComplete} 는 애초에 등록되지 않고, 등록됐다면 아직 완료되지
 * 않은 future 라 {@code catch} 로 오지 않는다.
 *
 * <p><b>{@code Error} 는 다시 던진다.</b> "관측이 발급을 죽이면 안 된다" 는 원칙은 업무 예외에
 * 대한 것이지, JVM 이 죽어 가는 상황까지 숨기라는 뜻이 아니다. {@code OutOfMemoryError} 를
 * 삼키고 발급을 계속하면 이미 불건전한 프로세스가 요청을 계속 받는다.
 *
 * <p>결과를 <b>기다리지는</b> 않는다. {@code whenComplete} 는 콜백을 걸 뿐이고,
 * {@code get()} 을 부르는 순간 톰캣 워커가 브로커 응답을 기다리게 되어 이 계층의 존재
 * 이유가 사라진다.
 */
@Component
@ConditionalOnProperty("kafka.enabled")
public class AttemptEventPublisher implements EventRecorder {

    private final KafkaTemplate<String, Object> template;
    private final AttemptFailureCounter failures;

    public AttemptEventPublisher(
            @Qualifier("attemptKafkaTemplate") KafkaTemplate<String, Object> attemptKafkaTemplate,
            AttemptFailureCounter attemptFailureCounter
    ) {
        this.template = Objects.requireNonNull(attemptKafkaTemplate, "attemptKafkaTemplate");
        this.failures = Objects.requireNonNull(attemptFailureCounter, "attemptFailureCounter");
    }

    @Override
    public void record(IssuanceFlowEvent event) {
        try {
            template.send(KafkaTopicConfig.ISSUE_ATTEMPT, PartitionKeys.forAttempt(event), event)
                    .whenComplete((result, failure) -> {
                        if (failure != null) {
                            failures.record(failure, event);
                        }
                    });
        } catch (Error fatal) {
            throw fatal;
        } catch (Throwable failure) {
            // 발급 경로다. 여기서 나가는 예외는 전부 발급 실패로 읽힌다.
            failures.record(failure, event);
        }
    }
}
