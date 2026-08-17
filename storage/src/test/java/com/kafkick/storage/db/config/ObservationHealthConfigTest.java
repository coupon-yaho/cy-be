package com.kafkick.storage.db.config;

import static org.assertj.core.api.Assertions.assertThat;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.jdbc.autoconfigure.health.DataSourceHealthContributorAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;


/**
 * 관측 풀 장애가 /actuator/health 를 DOWN 으로 만들면, 발급 API 는 멀쩡한데 인스턴스가 빠진다.
 * 이 설정이 없으면 Boot 가 DataSource 빈을 전부 묶어 합성하므로 여기서 못을 박는다.
 */
class ObservationHealthConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(ObservationDataSourceConfig.class, ObservationHealthConfig.class)
        .withConfiguration(AutoConfigurations.of(DataSourceHealthContributorAutoConfiguration.class))
        .withPropertyValues(
            "spring.datasource.url=jdbc:mysql://localhost:3306/app",
            "spring.datasource.username=app",
            "spring.datasource.password=app",
            // 아무도 안 듣는 포트다. 로컬에 MySQL 이 떠 있어도 결과가 바뀌지 않아야 한다.
            "observation.datasource.url=jdbc:mysql://localhost:1/app",
            "observation.datasource.username=obs",
            "observation.datasource.password=obs",
            "observation.datasource.hikari.connection-timeout=250",
            "observation.datasource.hikari.initialization-fail-timeout=-1");

    @Test
    void 기본_헬스체크_대상은_운영_풀_하나뿐이다() {
        runner.run(context -> {
            HealthContributor contributor = context.getBean("dbHealthContributor", HealthContributor.class);

            assertThat(context.getBeanNamesForType(DataSource.class)).hasSize(2);
            assertThat(contributor).isNotInstanceOf(CompositeHealthContributor.class);
        });
    }

    /**
     * 감시에서 빼기만 하면 반대쪽 구멍이 난다 — 비밀번호 오타로 배포해도 기동과 헬스가 통과하고
     * 대시보드 첫 조회에서야 드러난다. 그래서 별도 기여자로 남기되 상태를 낮추지 않는다.
     */
    @Test
    void 관측_풀은_별도_기여자로_감시된다() {
        runner.run(context -> assertThat(context).hasBean("obsDbHealthContributor"));
    }

    @Test
    void 관측_풀_장애는_DOWN_이_아니라_전용_상태로_보고된다() {
        runner.run(context -> {
            HealthIndicator obs = (HealthIndicator) context.getBean("obsDbHealthContributor");

            Health health = obs.health();

            // 접속 대상이 없으므로 실패한다. 그때 DOWN 이면 합산 상태가 내려가 인스턴스가 빠진다.
            assertThat(health.getStatus()).isEqualTo(ObservationHealthConfig.OBSERVATION_DOWN);
            assertThat(health.getStatus()).isNotEqualTo(Status.DOWN);
        });
    }

    /**
     * 위임 대상이 담는 details 는 원본 예외 메시지라 계정명·JDBC URL 이 그대로 실린다.
     * {@code show-details: never} 도 같은 것을 막지만 그 설정은 api 가 소유한 다른 파일에 있어서,
     * 이 코드가 보장하는 범위는 여기까지다.
     */
    @Test
    void 관측_풀_실패_응답에는_원본_details_가_없다() {
        runner.run(context -> {
            HealthIndicator obs = (HealthIndicator) context.getBean("obsDbHealthContributor");

            assertThat(obs.health().getDetails()).isEmpty();
        });
    }

    /**
     * management.yml 은 커밋하지 않는다. 이 PR 이전 파일을 그대로 가진 사람은 순서 목록 없이 뜨고,
     * 그러면 관측 풀 장애가 그대로 DOWN 으로 합산돼 인스턴스가 빠진다 — 에러도 경고도 없이.
     */
    @Test
    void 낡은_management_설정으로는_기동하지_못한다() {
        runner.withPropertyValues(
                "management.endpoint.health.show-details=never",
                "management.endpoint.health.status.order=DOWN,OUT_OF_SERVICE,UP,UNKNOWN")
            .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void 순서_목록에_관측_상태가_있으면_기동한다() {
        runner.withPropertyValues(
                "management.endpoint.health.show-details=never",
                "management.endpoint.health.status.order=DOWN,OUT_OF_SERVICE,UP,OBSERVATION_DOWN,UNKNOWN")
            .run(context -> assertThat(context).hasNotFailed());
    }

    /**
     * <b>알면서 남긴 구멍이다.</b> 관리 설정을 아예 안 하는 JVM 까지 막으면 관측과 무관한 것이 죽는다
     * (설정 파일 없이 뜨는 테스트 컨텍스트가 그렇다). 낡은 파일은 show-details 같은 키를 갖고 있어서
     * 위 테스트의 조건으로 걸린다 — 이 테스트는 그 경계가 어디인지를 기록해 둔다.
     */
    @Test
    void 관리_설정이_아예_없으면_검사하지_않는다() {
        runner.run(context -> assertThat(context).hasNotFailed());
    }
}
