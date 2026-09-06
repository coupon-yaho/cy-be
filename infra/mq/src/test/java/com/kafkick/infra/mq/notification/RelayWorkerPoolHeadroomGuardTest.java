package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * <b>경계는 실측에서 왔다.</b> 배포와 같은 조건(풀 13, <b>요청 스레드 15</b>)에서
 * 워커 9(접수 몫 넷)는 워커 8 과 구분이 안 되고, 워커 10(셋)에서 p99 분산이 터진다
 * (1,595~5,451µs). 이 테스트가 그 두 점을 그대로 태운다.
 *
 * <p>⚠️ 첫 판은 <b>요청 스레드 하나</b>로 재서 경계를 11→12 로 잡았고 헤드룸이 2 였다.
 * 배포의 톰캣 워커는 15 다 — <b>재는 조건이 실제와 다르면 "실측" 도 틀린다.</b>
 *
 * <p>상수만 검사하면 안 된다. {@code REQUEST_HEADROOM} 을 1 이나 3 으로 바꾸는 돌연변이가
 * <b>둘 중 한쪽에서 걸려야</b> 경계를 실제로 지키는 것이다.
 */
class RelayWorkerPoolHeadroomGuardTest {

    private static final String GAUGE = "cy_notify_relay_pool_headroom_verified";

    /** 커넥션을 열지 않는다 — 가드도 열면 안 된다. 열면 여기서 터진다. */
    private static class PoolStub implements DataSource {
        private final int maximumPoolSize;

        PoolStub(int maximumPoolSize) {
            this.maximumPoolSize = maximumPoolSize;
        }

        @SuppressWarnings("unused") // 리플렉션으로 찾는다
        public int getMaximumPoolSize() {
            return maximumPoolSize;
        }

        @Override
        public Connection getConnection() {
            throw new AssertionError("가드가 커넥션을 열었다 — 설정값만 읽어야 한다");
        }

        @Override
        public Connection getConnection(String username, String password) {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }

        @Override
        public <T> T unwrap(Class<T> type) throws SQLException {
            throw new SQLException("래핑하지 않는다");
        }

        @Override
        public boolean isWrapperFor(Class<?> type) {
            return false;
        }
    }

    /** {@code getMaximumPoolSize} 가 없는 구현. 풀이 아닌 DataSource 는 흔하다. */
    private static class NotAPool extends PoolStub {
        NotAPool() {
            super(0);
        }

        @Override
        public int getMaximumPoolSize() {
            throw new UnsupportedOperationException("이 구현에는 없다");
        }
    }

    private static double gauge(MeterRegistry registry) {
        return registry.get(GAUGE).gauge().value();
    }

    /**
     * <b>잰 좋은 점이다.</b> 풀 13 · 워커 9 → 요청 p99 1,225~1,695µs 로 워커 8 과 구분이
     * 안 된다. 여기서 거절하면 <b>멀쩡한 설정이 기동을 거부당한다</b> — 가드가 성능을
     * 정하는 자리가 아니다.
     */
    @Test
    @DisplayName("접수 몫이 넷 남으면 통과한다 — 실측에서 기본값과 구분 안 되는 지점")
    void passesWhenFourConnectionsRemainForRequests() {
        MeterRegistry registry = new SimpleMeterRegistry();

        assertThatCode(() -> new RelayWorkerPoolHeadroomGuard(new PoolStub(13), 9, true, registry))
                .doesNotThrowAnyException();
        assertThat(gauge(registry)).isEqualTo(1);
    }

    /**
     * <b>잰 나쁜 점이다.</b> 풀 13 · 워커 10 → 요청 p99 가 1,595~5,451µs 로 <b>세 배 넘게
     * 흔들린다</b>(max 48ms). 앱은 정상으로 뜨고 접수만 느려지므로, 조용히 통과시키면
     * 아무도 못 알아챈다.
     *
     * <p><b>평균이 아니라 분산이 무너지는 것이 신호다.</b> 한 회차만 보면 1,595µs 라
     * 멀쩡해 보인다 — 그래서 회차를 셋 돌렸다.
     */
    @Test
    @DisplayName("접수 몫이 셋뿐이면 기동에서 거절한다 — 실측에서 분산이 터지는 지점")
    void refusesWhenOnlyThreeConnectionsRemainForRequests() {
        MeterRegistry registry = new SimpleMeterRegistry();

        assertThatThrownBy(() -> new RelayWorkerPoolHeadroomGuard(new PoolStub(13), 10, true, registry))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("워커를 9 이하로")
                .hasMessageContaining("DB_POOL_SIZE 를 14 이상으로");
    }

    /** 배포 기본값(워커 8 · 풀 13)이 통과해야 한다 — 안 그러면 아무도 못 띄운다. */
    @Test
    @DisplayName("배포 기본값 조합은 통과한다")
    void passesTheDeployedDefaults() {
        MeterRegistry registry = new SimpleMeterRegistry();

        assertThatCode(() -> new RelayWorkerPoolHeadroomGuard(new PoolStub(13), 8, true, registry))
                .doesNotThrowAnyException();
        assertThat(gauge(registry)).isEqualTo(1);
    }

    /**
     * <b>여기서 기동을 막으면 풀 구현을 바꾸는 날 접수 API 가 통째로 안 뜬다.</b>
     * 못 읽는 것은 설정이 틀렸다는 뜻이 아니다 — 통과시키되 지표에 0 으로 남긴다.
     */
    @Test
    @DisplayName("풀 크기를 못 읽으면 막지 않고 지표에 남긴다")
    void skipsWhenThePoolSizeCannotBeRead() {
        MeterRegistry registry = new SimpleMeterRegistry();

        assertThatCode(() -> new RelayWorkerPoolHeadroomGuard(new NotAPool(), 999, true, registry))
                .doesNotThrowAnyException();
        assertThat(gauge(registry))
                .as("검사를 못 했다는 것이 통과와 같은 값으로 보이면 안 된다")
                .isEqualTo(0);
    }

    /**
     * <b>끄는 손잡이가 없으면 되돌릴 방법이 없다.</b> 이 가드가 막는 것이 접수 API 의
     * 기동이라, 잘못 걸렸을 때 배포를 못 되돌리는 상황이 된다.
     *
     * <p>다만 <b>끈 상태가 조용하면 안 된다</b> — 이 스택에는 로그 수집이 없어서, 지표가
     * 0 으로 남는 것이 유일한 감시 수단이다.
     */
    @Test
    @DisplayName("거절을 끄면 기동은 하되 지표가 0 으로 남는다")
    void whenTheGuardIsTurnedOffItStillReportsThroughTheGauge() {
        MeterRegistry registry = new SimpleMeterRegistry();

        assertThatCode(() -> new RelayWorkerPoolHeadroomGuard(new PoolStub(13), 10, false, registry))
                .doesNotThrowAnyException();
        assertThat(gauge(registry)).isEqualTo(0);
    }

    /** 지표 등록기가 없는 컨텍스트에서도 검사 자체는 돌아야 한다. */
    @Test
    @DisplayName("MeterRegistry 가 없어도 검사는 돈다")
    void worksWithoutAMeterRegistry() {
        assertThatThrownBy(() -> new RelayWorkerPoolHeadroomGuard(new PoolStub(13), 10, true, null))
                .isInstanceOf(IllegalStateException.class);
        assertThatCode(() -> new RelayWorkerPoolHeadroomGuard(new PoolStub(13), 8, true, null))
                .doesNotThrowAnyException();
    }
}
