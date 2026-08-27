package com.kafkick.storage.db.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.jdbc.health.DataSourceHealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * 헬스체크에서 운영 풀과 관측 풀을 갈라 놓는다.
 *
 * <p>Boot 의 {@code dbHealthContributor} 는 컨텍스트의 DataSource 빈을 <b>전부</b> 묶어 합성한다.
 * 관측 풀이 생긴 지금 그대로 두면 관측 풀 장애만으로 /actuator/health 가 DOWN 이 되어, 발급 API 는
 * 멀쩡한데 인스턴스가 로드밸런서에서 빠진다. 관측이 측정 대상을 바꾸는 상황 그 자체다.
 *
 * <p>풀을 만든 모듈이 그 풀의 신호도 낸다. 신호를 어떻게 노출하고 로드밸런서가 무엇을 보는지는
 * api 의 management 설정이 정한다 — 생성은 여기, 해석은 거기다.
 *
 * <p>조건이 둘이다. actuator 가 없는 JVM 에서는 health 클래스가 없어 건너뛰고, 관측을 켜지 않은
 * 모듈에서는 나눌 풀 자체가 없어 건너뛴다. 후자를 빠뜨리면 관측 풀이 없는데도 기본 기여자를
 * 대체해 버려서, 관측과 무관한 모듈의 헬스체크 동작을 이유 없이 바꾼다.
 *
 * <p>그렇다고 관측 풀을 감시에서 빼기만 하면 반대쪽 구멍이 난다 — 비밀번호를 오타 낸 채 배포해도
 * 기동은 성공하고(Hikari 는 첫 조회까지 커넥션을 안 연다) 헬스도 UP 이라, 대시보드 첫 조회에서야
 * 드러난다. 그래서 관측 풀은 <b>상태를 낮추지 않는 별도 status</b> 로 보고하고
 * {@code /actuator/health/obs} 그룹에서 따로 본다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(HealthContributor.class)
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class ObservationHealthConfig {

    /**
     * 이 코드 문자열은 <b>management.yml 의 status.order · obs 그룹 http-mapping 과 한 쌍</b>이다.
     * 여기만 바꾸면 순서 목록에서 빠져 합산 상태가 UP 이 아니게 되고, 거기만 바꾸면 매핑이 어긋난다.
     * 둘을 잇는 검증은 api 의 {@code ObservationHealthContractTest} 가 한다.
     */
    public static final Status OBSERVATION_DOWN = new Status("OBSERVATION_DOWN");

    /** management.yml 의 {@code group.<이름>} 과 같아야 한다. */
    static final String HEALTH_GROUP = "obs";

    /**
     * management.yml 의 {@code group.obs.include} 가 지목하는 이름. Boot 가 기여자 빈 이름에서
     * {@code HealthContributor} 접미사를 떼어 만든다 — 즉 아래 {@code obsDbHealthContributor}
     * 메서드 이름과 한 쌍이다.
     *
     * <p>메서드 이름만 바꾸면 이 상수와 어긋나는데, 그때는 Boot 자신이 "그룹이 지목한 기여자가
     * 없다" 며 기동을 멈춘다. 조용히 깨지지 않으므로 여기서 따로 검사하지 않는다.
     */
    static final String CONTRIBUTOR_ID = "obsDb";

    /** 빈 이름이 {@code dbHealthContributor} 여야 전 DataSource 를 묶던 자동설정이 물러난다. */
    @Bean
    public HealthContributor dbHealthContributor(DataSource dataSource) {
        return new DataSourceHealthIndicator(dataSource);
    }

    /**
     * 상태를 만드는 쪽과 해석하는 쪽이 다른 파일이라, 해석 쪽이 낡았는지 기동 시점에 확인한다.
     * 자세한 조건과 알면서 남긴 구멍은 {@link ObservationHealthStatusOrderCheck} 에 적었다.
     */

    @Bean
    ObservationHealthStatusOrderCheck observationHealthStatusOrderCheck(Environment environment) {
        return new ObservationHealthStatusOrderCheck(environment);
    }

    @Bean
    public HealthContributor obsDbHealthContributor(@Qualifier("obs") DataSource observationDataSource) {
        return new ObservationPoolHealthIndicator(new DataSourceHealthIndicator(observationDataSource));
    }

    /** DOWN 을 그대로 내보내면 합산 상태가 내려가 로드밸런서가 인스턴스를 뺀다. */
    private record ObservationPoolHealthIndicator(DataSourceHealthIndicator delegate) implements HealthIndicator {

        /**
         * 실패 details 는 옮기지 않는다. 위임 대상이 담는 값은 원본 예외 메시지라 계정명·JDBC URL 같은
         * 접속 정보가 그대로 실린다. {@code show-details: never} 가 지금은 막고 있지만, 그 설정은
         * api 가 소유한 다른 파일에 있어서 여기 코드가 보장하지 못한다.
         *
         * <p>대신 잃는 것 — 실패 원인은 응답에서 사라진다. 원인을 볼 곳은 WARN 로그 하나뿐이다:
         * 로거 {@code org.springframework.boot.jdbc.health.DataSourceHealthIndicator},
         * 메시지 {@code "DataSource health check failed"} + 원본 예외.
         * ({@code AbstractHealthIndicator} 가 {@code getLog(getClass())} 로 만들어서 로거 이름은
         * 추상 클래스가 아니라 구현 클래스다.)
         */
        @Override
        public Health health() {
            Health health = delegate.health();
            if (Status.UP.equals(health.getStatus())) {
                return health;
            }
            return Health.status(OBSERVATION_DOWN).build();
        }
    }
}
