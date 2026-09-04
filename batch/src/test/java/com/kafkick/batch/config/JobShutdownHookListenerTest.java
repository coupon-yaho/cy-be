package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

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

    private final FakeShutdownHooks jvm = new FakeShutdownHooks();

    private JobShutdownHookListener listener(Job... jobs) {
        return new JobShutdownHookListener(provider(operator), provider(jobs), jvm);
    }

    /**
     * <b>테스트가 진짜 {@code Runtime} 에 훅을 붙이면 안 된다.</b> 붙은 채로 테스트가 끝나면
     * 테스트 JVM 이 죽을 때까지 남고, 그 훅이 죽은 실행을 가리킨다.
     *
     * <p>그리고 <b>catch 두 개를 태우려면 실패를 만들 수 있어야 한다</b> — 실제 등록 거부는
     * JVM 이 종료 중이어야만 나므로 테스트가 만들 수 없다.
     */
    private static final class FakeShutdownHooks
            implements JobShutdownHookListener.ShutdownHooks {
        private final Set<Thread> registered = new HashSet<>();
        private RuntimeException addFailure;
        private RuntimeException removeFailure;

        @Override
        public void add(Thread hook) {
            if (addFailure != null) {
                throw addFailure;
            }
            if (!registered.add(hook)) {
                throw new IllegalArgumentException("Hook previously registered");
            }
        }

        @Override
        public void remove(Thread hook) {
            if (removeFailure != null) {
                throw removeFailure;
            }
            registered.remove(hook);
        }

        int size() {
            return registered.size();
        }
    }

    private JobShutdownHookListener listener() {
        return listener(attached);
    }

    private JobShutdownHookListener subject;

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
        jvm.removeFailure = new IllegalStateException("Shutdown in progress");

        assertThatCode(() -> subject.afterJob(execution)).doesNotThrowAnyException();
        assertThat(subject.registeredHookCount())
                .as("해제에 실패해도 맵에서는 빠져야 합니다 — 안 빠지면 다음 실행이 못 붙습니다")
                .isZero();
    }

    /** 맵에 없는 실행의 {@code afterJob} 은 아무 일도 안 한다. */
    @Test
    void ignoresAfterJobForAnExecutionItNeverRegistered() {
        subject = listener();

        assertThatCode(() -> subject.afterJob(execution(99L))).doesNotThrowAnyException();
    }

    /**
     * 훅을 못 붙여도 잡은 계속 돌아야 한다 — 관측 장치가 업무를 죽이면 안 된다.
     *
     * <p><b>실제로 등록을 거부시킨다.</b> 예전에는 같은 실행으로 {@code beforeJob} 을 두 번
     * 불러 흉내내려 했는데, 호출마다 새 {@code Thread} 를 만들므로 <b>두 번째도 정상 등록됐다</b>
     * — catch 를 한 번도 안 태우면서 통과하던 테스트였다(Qodo 리뷰가 잡았다).
     */
    @Test
    void doesNotFailTheJobWhenTheHookCannotBeRegistered() {
        subject = listener();
        jvm.addFailure = new IllegalStateException("Shutdown in progress");

        assertThatCode(() -> subject.beforeJob(execution(1L))).doesNotThrowAnyException();
        assertThat(subject.registeredHookCount())
                .as("등록에 실패했는데 맵에 남으면 afterJob 이 없는 훅을 떼려 한다")
                .isZero();
    }

    /** 같은 실행에 두 번 붙으면 앞 훅을 뗀다 — 안 떼면 JVM 이 죽을 때까지 샌다. */
    @Test
    void removesThePreviousHookWhenTheSameExecutionIsRegisteredTwice() {
        subject = listener();
        JobExecution execution = execution(1L);

        subject.beforeJob(execution);
        subject.beforeJob(execution);

        assertThat(jvm.size())
                .as("앞 훅이 JVM 에 남았습니다")
                .isEqualTo(1);
        assertThat(subject.registeredHookCount()).isEqualTo(1);
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
