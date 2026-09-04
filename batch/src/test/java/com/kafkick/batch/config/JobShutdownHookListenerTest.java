package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.ObjectProvider;

/**
 * 이 리스너가 지키는 것은 둘이다 — <b>훅이 누적되지 않는 것</b>과
 * <b>훅 문제가 잡을 죽이지 않는 것</b>.
 */
class JobShutdownHookListenerTest {

    private final JobOperator operator = mock(JobOperator.class);
    private final Job[] attached = new Job[] {};

    private JobShutdownHookListener listener(Job... jobs) {
        return new JobShutdownHookListener(provider(operator), provider(jobs));
    }

    private JobShutdownHookListener listener() {
        return listener(attached);
    }

    private JobShutdownHookListener subject;

    @AfterEach
    void cleanUp() {
        // 테스트가 남긴 훅을 JVM 에 두고 가지 않는다.
        if (subject != null) {
            subject.afterJob(execution(1L));
            subject.afterJob(execution(2L));
        }
    }

    @Test
    void registersAHookWhileTheJobRuns() {
        subject = listener();

        subject.beforeJob(execution(1L));

        assertThat(subject.registeredHookCount()).isEqualTo(1);
    }

    /**
     * <b>안 떼면 실행 수만큼 쌓인다.</b> {@code Runtime} 이 붙잡은 {@code Thread} 참조라
     * JVM 이 죽을 때까지 안 없어지고, 끝난 실행을 가리키는 죽은 훅이 SIGTERM 에 전부 깨어난다.
     */
    @Test
    void removesTheHookWhenTheJobEndsSoTheyDoNotAccumulate() {
        subject = listener();

        for (long id = 1; id <= 50; id++) {
            JobExecution execution = execution(id);
            subject.beforeJob(execution);
            subject.afterJob(execution);
        }

        assertThat(subject.registeredHookCount()).isZero();
    }

    /** 동시에 도는 잡이 서로의 훅을 떼면 안 된다 — verify 는 전용 실행기에서 비동기다. */
    @Test
    void keepsHooksSeparatePerExecution() {
        subject = listener();
        JobExecution first = execution(1L);
        JobExecution second = execution(2L);

        subject.beforeJob(first);
        subject.beforeJob(second);
        subject.afterJob(first);

        assertThat(subject.registeredHookCount()).isEqualTo(1);
    }

    /**
     * <b>afterJob 이 종료 시퀀스 안에서 도는 것이 정상 경로다.</b> 훅이 잡을 멈추면 그렇게
     * 되고, 그때 {@code removeShutdownHook} 은 언제나 {@code IllegalStateException} 을 던진다.
     * 그것이 밖으로 나가면 <b>정상 종료가 예외로 끝난다.</b>
     */
    @Test
    void swallowsRemovalFailureDuringShutdown() {
        subject = listener();
        JobExecution execution = execution(1L);
        subject.beforeJob(execution);

        // 훅을 몰래 떼어 두면 removeShutdownHook 이 false 를 돌려주고, 실제 종료 중이면
        // IllegalStateException 이 난다. 둘 다 밖으로 나가면 안 된다.
        assertThatCode(() -> {
            subject.afterJob(execution);
            subject.afterJob(execution);   // 두 번째는 맵에 없다
        }).doesNotThrowAnyException();
    }

    /** 훅을 못 붙여도 잡은 계속 돌아야 한다 — 관측 장치가 업무를 죽이면 안 된다. */
    @Test
    void doesNotFailTheJobWhenTheHookCannotBeRegistered() {
        subject = listener();
        JobExecution execution = execution(1L);

        subject.beforeJob(execution);
        // 같은 실행으로 다시 부르면 addShutdownHook 이 IllegalArgumentException 을 던지는
        // 상황을 흉내낸다. 맵이 덮이더라도 예외는 밖으로 안 나간다.
        assertThatCode(() -> subject.beforeJob(execution)).doesNotThrowAnyException();
    }

    /**
     * <b>새 잡이 자동으로 따라와야 한다.</b> 잡 빌더에 손으로 붙이면 다음 사람이 빠뜨리고,
     * 그 잡만 SIGTERM 에 시체를 남긴다 — 관제에서는 "가끔 굳는다" 로만 보인다.
     */
    @Test
    void attachesItselfToEveryJobBean() {
        SimpleJob one = new SimpleJob("one");
        SimpleJob two = new SimpleJob("two");
        JobShutdownHookListener registrar = listener(one, two);

        registrar.afterSingletonsInstantiated();

        assertThat(listenersOf(one)).contains(registrar);
        assertThat(listenersOf(two)).contains(registrar);
    }

    /** {@code AbstractJob} 이 아닌 구현이 있어도 기동이 깨지지 않는다. */
    @Test
    void toleratesJobsItCannotAttachTo() {
        Job opaque = mock(Job.class);
        JobShutdownHookListener registrar = listener(opaque);

        assertThatCode(registrar::afterSingletonsInstantiated).doesNotThrowAnyException();
    }

    private static JobExecution execution(long id) {
        return new JobExecution(id, new JobInstance(id, "job-" + id), new JobParameters());
    }

    /**
     * {@code AbstractJob} 의 리스너는 {@code CompositeJobExecutionListener} 안의
     * {@code OrderedComposite} 에 있다. 그 필드는 {@code List} 가 아니라 합성체 자신이고,
     * 공개된 것은 {@code iterator()} 뿐이라 그것으로 읽는다.
     */
    private static List<Object> listenersOf(AbstractJob job) {
        try {
            var field = AbstractJob.class.getDeclaredField("listener");
            field.setAccessible(true);
            Object composite = field.get(job);
            var listeners = composite.getClass().getDeclaredField("listeners");
            listeners.setAccessible(true);
            Object ordered = listeners.get(composite);
            var iterator = ordered.getClass().getDeclaredMethod("iterator");
            iterator.setAccessible(true);
            List<Object> found = new ArrayList<>();
            ((Iterator<?>) iterator.invoke(ordered)).forEachRemaining(found::add);
            return found;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("리스너 합성체 구조가 바뀌었습니다.", e);
        }
    }

    @SafeVarargs
    private static <T> ObjectProvider<T> provider(T... values) {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                return values[0];
            }

            @Override
            public Stream<T> stream() {
                return Stream.of(values);
            }
        };
    }
}
