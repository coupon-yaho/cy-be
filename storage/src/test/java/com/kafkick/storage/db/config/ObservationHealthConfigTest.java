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

    /** management.yml.example 이 선언하는 관측 계약 한 벌. 하나라도 빠지면 기동을 막아야 한다. */
    private static final String[] MANAGEMENT_YML = {
        "management.endpoint.health.show-details=never",
        "management.endpoint.health.status.order=DOWN,OUT_OF_SERVICE,UP,OBSERVATION_DOWN,UNKNOWN",
        "management.endpoint.health.group.obs.include=obsDb",
        "management.endpoint.health.group.obs.status.http-mapping.OBSERVATION_DOWN=503",
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        // CY-338: 운영 풀은 MainDataSourceConfig 로 옮겼다. dbHealthContributor 가 그것을 받는다.
        .withUserConfiguration(MainDataSourceConfig.class, ObservationDataSourceConfig.class,
            ObservationHealthConfig.class)
        .withConfiguration(AutoConfigurations.of(DataSourceHealthContributorAutoConfiguration.class))
        .withPropertyValues(
            "observation.datasource.enabled=true",
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
        runner.withPropertyValues(MANAGEMENT_YML)
            .withPropertyValues(order("DOWN,OUT_OF_SERVICE,UP,UNKNOWN"))
            .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void 계약을_전부_갖춘_management_설정이면_기동한다() {
        runner.withPropertyValues(MANAGEMENT_YML).run(context -> assertThat(context).hasNotFailed());
    }

    /**
     * 합산 상태는 목록에서 가장 앞선 것으로 정해진다. 관측 상태가 UP 앞에 오면 관측 풀 장애만으로
     * 인스턴스가 로드밸런서에서 빠진다 — 이 티켓이 막으려던 사고 그 자체다.
     *
     * <p>포함 여부만 보던 때는 이 설정이 통과했다.
     */
    @Test
    void 관측_상태가_UP_보다_앞서면_기동하지_못한다() {
        runner.withPropertyValues(MANAGEMENT_YML)
            .withPropertyValues(order("OBSERVATION_DOWN,DOWN,OUT_OF_SERVICE,UP,UNKNOWN"))
            .run(context -> assertThat(context).hasFailed());
    }

    /**
     * <b>반대 방향이 더 큰 사고다.</b> UP 이 맨 앞이면 운영 DB 가 죽어도 합산이 UP 이라
     * 헬스체크가 200 을 돌려준다(실측). 관측 상태는 UP 뒤라 "관측만 보는" 검사는 통과한다.
     */
    @Test
    void 운영_실패_상태가_UP_보다_뒤면_기동하지_못한다() {
        runner.withPropertyValues(MANAGEMENT_YML)
            .withPropertyValues(order("UP,DOWN,OUT_OF_SERVICE,OBSERVATION_DOWN,UNKNOWN"))
            .run(context -> assertThat(context).hasFailed());
    }

    /**
     * 목록에 없는 상태는 "가장 안 심각" 으로 취급된다(실측). 그래서 DOWN 이 빠진 것은 DOWN 이
     * UP 뒤에 있는 것과 결과가 같다 — 위치만 비교하는 검사는 이걸 놓친다(index 가 -1 이라
     * "UP 보다 앞" 으로 읽힌다).
     */
    @Test
    void 운영_실패_상태가_목록에_없으면_기동하지_못한다() {
        runner.withPropertyValues(MANAGEMENT_YML)
            .withPropertyValues(order("OUT_OF_SERVICE,UP,OBSERVATION_DOWN,UNKNOWN"))
            .run(context -> assertThat(context).hasFailed());
    }

    /**
     * 기준점이 없으면 나머지 순서를 판정할 수 없다. 이때 UP 은 가장 안 심각해진다.
     *
     * <p><b>메시지까지 확인하는 이유</b> — UP 이 없으면 DOWN 검사도 어차피 걸린다(인덱스 -1 과
     * 비교되므로). 그래서 "기동 실패" 만 보면 UP 기준점 검사를 지워도 이 테스트가 통과한다 —
     * 실제로 지워 보고 확인했다. 그러면 진단 메시지가 "DOWN 이 UP 보다 앞이 아니다" 로 나가서,
     * 정작 빠진 것이 UP 인데 엉뚱한 곳을 보게 만든다.
     */
    @Test
    void UP_이_목록에_없으면_기동하지_못한다() {
        runner.withPropertyValues(MANAGEMENT_YML)
            .withPropertyValues(order("DOWN,OUT_OF_SERVICE,OBSERVATION_DOWN,UNKNOWN"))
            .run(context -> assertThat(context).getFailure()
                .rootCause()
                .hasMessageContaining("기준점"));
    }

    /**
     * 창구가 없으면 관측 풀 장애가 <b>어디에도 안 드러난다</b>. 합산 상태는 설계대로 UP 이라
     * 기본 헬스체크는 통과하고, 그룹이 그 상태를 보여 주는 유일한 자리이기 때문이다.
     */
    @Test
    void 그룹이_관측_기여자를_지목하지_않으면_기동하지_못한다() {
        runner.withPropertyValues(MANAGEMENT_YML)
            .withPropertyValues("management.endpoint.health.group.obs.include=db")
            .run(context -> assertThat(context).hasFailed());
    }

    /**
     * 가장 빠뜨리기 쉬운 자리다. OBSERVATION_DOWN 은 Boot 기본 매핑에 없는 상태라
     * <b>안 적으면 200</b> 이 되고, 그러면 그룹은 있는데 늘 정상이라고 답한다 — 경보가 안 울린다.
     */
    @Test
    void 그룹이_관측_상태를_실패로_매핑하지_않으면_기동하지_못한다() {
        runner.withPropertyValues(
                "management.endpoint.health.show-details=never",
                "management.endpoint.health.status.order=DOWN,OUT_OF_SERVICE,UP,OBSERVATION_DOWN,UNKNOWN",
                "management.endpoint.health.group.obs.include=obsDb")
            .run(context -> assertThat(context).hasFailed());
    }

    /** 200 을 명시하는 것도 안 적은 것과 같다 — 그룹이 늘 정상이라고 답한다. */
    @Test
    void 그룹_매핑이_성공_코드면_기동하지_못한다() {
        runner.withPropertyValues(MANAGEMENT_YML)
            .withPropertyValues("management.endpoint.health.group.obs.status.http-mapping.OBSERVATION_DOWN=200")
            .run(context -> assertThat(context).hasFailed());
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

    /**
     * 관측을 켜지 않은 모듈에서는 나눌 풀 자체가 없다. 그런데도 기본 기여자를 대체하면, 관측과
     * 무관한 모듈의 헬스체크 동작을 이유 없이 바꾸고 낡은 management.yml 까지 트집 잡는다.
     */
    @Test
    void 관측을_켜지_않으면_헬스_설정도_건너뛴다() {
        new ApplicationContextRunner()
            .withUserConfiguration(ObservationHealthConfig.class)
            .withPropertyValues(
                "management.endpoint.health.show-details=never",
                "management.endpoint.health.status.order=DOWN,OUT_OF_SERVICE,UP,UNKNOWN")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).doesNotHaveBean("dbHealthContributor");
                assertThat(context).doesNotHaveBean("obsDbHealthContributor");
                assertThat(context).doesNotHaveBean("observationHealthStatusOrderCheck");
            });
    }

    private static String order(String value) {
        return "management.endpoint.health.status.order=" + value;
    }

}
