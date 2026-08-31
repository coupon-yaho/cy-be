package com.kafkick.api.support;

import java.time.Clock;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import jakarta.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockHttpServletRequest;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.ErrorCode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 완화 응답(503)은 의존성 장애 동안 초당 수천 건이 된다. 요청마다 스택을 찍으면 로그 I/O 가
 * 응답 지연을 밀어 올려, 장애 대응이 아니라 장애 증폭이 된다.
 *
 * <p>이 사실은 스택 유무로만 증명된다 — 조건을 지워도 앱은 정상이고 에러 응답도 같다.
 */
class MitigatedFailureLoggingTest {

    private final Logger handlerLogger =
            (Logger) LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private final ListAppender<ILoggingEvent> events = new ListAppender<>();
    private final GlobalExceptionHandler handler =
            new GlobalExceptionHandler(new TimeProvider(Clock.systemUTC()));

    @BeforeEach
    void attachAppender() {
        events.start();
        handlerLogger.addAppender(events);
    }

    @AfterEach
    void detachAppender() {
        handlerLogger.detachAppender(events);
    }

    @Test
    void omitsTheStackTraceForAMitigated5xx() {
        handler.handleBusinessException(
                new BusinessException(new StubErrorCode(503, false), "완화", new IllegalStateException("db")),
                request());

        assertThat(errorEvents()).singleElement()
                .satisfies(event -> assertThat(event.getThrowableProxy()).isNull());
    }

    @Test
    void keepsTheStackTraceForAnUnexpected5xx() {
        handler.handleBusinessException(
                new BusinessException(new StubErrorCode(500, true), "예상 밖", new IllegalStateException("bug")),
                request());

        assertThat(errorEvents()).singleElement()
                .satisfies(event -> assertThat(event.getThrowableProxy()).isNotNull());
    }

    private List<ILoggingEvent> errorEvents() {
        return events.list.stream().filter(event -> event.getLevel() == Level.ERROR).toList();
    }

    private static HttpServletRequest request() {
        return new MockHttpServletRequest();
    }

    private record StubErrorCode(int status, boolean stack) implements ErrorCode {
        @Override public int getStatus() { return status; }
        @Override public String getCode() { return "TEST-001"; }
        @Override public String getMessage() { return "테스트"; }
        @Override public Dependency dependency() { return Dependency.NONE; }
        @Override public boolean logStackTrace() { return stack; }
    }
}
