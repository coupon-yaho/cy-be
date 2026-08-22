package com.kafkick.storage.db.config;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.health.contributor.Status;
import org.springframework.core.env.Environment;

/**
 * 낡은 {@code management.yml} 로 뜨는 것을 기동 시점에 막는다.
 *
 * <p>이 계약은 코드와 설정 파일에 걸쳐 있다 — {@link ObservationHealthConfig#OBSERVATION_DOWN} 과
 * {@code management.endpoint.health.status.order}. 그런데 {@code management.yml} 은 커밋하지 않으므로,
 * 이 PR 이전의 파일을 그대로 가진 사람은 <b>순서 목록 없이</b> 뜬다. 그러면 관측 풀 장애가 그대로
 * {@code DOWN} 으로 합산되어 발급 API 가 멀쩡한데 인스턴스가 로드밸런서에서 빠진다 — 에러도 경고도
 * 없이 그렇게 된다. 이 티켓이 막으려던 것 그 자체다.
 *
 * <p><b>왜 storage 인가.</b> 검사 대상은 api 가 소유한 설정이지만, api 의 storage 의존은
 * {@code runtimeOnly} 라 api 본 코드가 이 상수를 참조할 수 없다. api 에 두면 문자열 사본이 세 번째로
 * 생겨 지금 고친 문제가 그대로 재발한다. 상수를 아는 쪽이 검사도 한다.
 *
 * <p><b>검사 범위 — 알면서 남긴 구멍.</b> {@code management.endpoint.health} 아래에 설정이
 * <b>하나라도</b> 있을 때만 검사한다. 관리 설정을 아예 안 하는 JVM(설정 파일 없이 뜨는 테스트
 * 컨텍스트가 그렇다)까지 막으면, 이 검사가 관측과 무관한 것을 죽인다. 낡은 파일은 최소한
 * {@code show-details} 같은 키를 갖고 있으므로 이 조건으로 걸린다.
 *
 * <p><b>일반 검증기가 아니다.</b> management 설정 전반을 검사하지 않고, 관측 계약이 성립하는 데
 * 필요한 것만 본다. 우리 것이 아닌 {@code DOWN}·{@code OUT_OF_SERVICE} 의 서열까지 보는 이유는
 * 하나뿐이다 — 이 PR 이전에는 아무도 {@code status.order} 를 건드릴 이유가 없었는데 우리가 그 줄을
 * 만들었고, 그래서 그 줄을 잘못 쓸 확률을 우리가 올렸다.
 *
 * <p><b>고르지 않은 대안</b> — 순서를 설정 파일이 아니라 코드가 정하게 하면({@code StatusAggregator}
 * 빈을 직접 정의) 어긋날 일 자체가 없어진다. 그러나 그 빈을 정의하는 순간
 * {@code management.endpoint.health.status.order} 설정이 <b>조용히 무시된다</b> — 자동설정을 대체할 때
 * 반복해서 겪은 그 구조다. 드리프트 위험을 "설정이 무시되는" 위험과 맞바꾸는 것이라 택하지 않았다.
 */
class ObservationHealthStatusOrderCheck implements InitializingBean {

    static final String HEALTH_PREFIX = "management.endpoint.health";

    static final String STATUS_ORDER = HEALTH_PREFIX + ".status.order";

    private static final String GROUP = HEALTH_PREFIX + ".group." + ObservationHealthConfig.HEALTH_GROUP;

    static final String GROUP_INCLUDE = GROUP + ".include";

    static final String GROUP_MAPPING = GROUP + ".status.http-mapping";

    /** 200 을 돌려주는 그룹은 경보를 걸 수 없다. 실패로 보여야 한다. */
    private static final int FAILURE_CODE_FLOOR = 400;

    private final Environment environment;

    ObservationHealthStatusOrderCheck(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void afterPropertiesSet() {
        Binder binder = Binder.get(environment);
        Map<String, Object> health = binder.bind(HEALTH_PREFIX, Bindable.mapOf(String.class, Object.class))
            .orElseGet(Map::of);
        if (health.isEmpty()) {
            return;
        }

        String code = ObservationHealthConfig.OBSERVATION_DOWN.getCode();
        requireSeverityOrder(binder, code);
        requireGroupMembership(binder);
        requireGroupReportsFailure(binder, code);
    }

    /**
     * 합산 상태는 이 목록에서 <b>가장 앞선 것</b>으로 정해진다. 그래서 우리 계약은 서열이다:
     *
     * <pre>DOWN · OUT_OF_SERVICE  &lt;  UP  &lt;  OBSERVATION_DOWN</pre>
     *
     * <p>포함 여부만 보면 부족하다. {@code OBSERVATION_DOWN} 이 {@code UP} 앞에 오면 관측 풀
     * 장애만으로 인스턴스가 빠지고(실측), 반대로 {@code UP} 이 맨 앞이면 <b>운영 DB 가 죽어도
     * 200 을 돌려준다</b>(실측). 뒤쪽이 더 큰 사고라 양쪽을 다 본다.
     *
     * <p><b>목록에 없는 상태는 "가장 안 심각" 으로 취급된다</b>(실측). 그래서 {@code DOWN} 이
     * 빠진 것과 {@code DOWN} 이 {@code UP} 뒤에 있는 것은 결과가 같다 — 같은 실패로 다룬다.
     */
    private static void requireSeverityOrder(Binder binder, String code) {
        List<String> order = binder.bind(STATUS_ORDER, Bindable.listOf(String.class)).orElseGet(List::of);

        int up = order.indexOf(Status.UP.getCode());
        if (up < 0) {
            throw stale(STATUS_ORDER + " 에 " + Status.UP.getCode() + " 가 없다."
                + " 기준점이 없으면 나머지 순서를 판정할 수 없다. (현재 값: " + order + ")");
        }

        int observation = order.indexOf(code);
        if (observation < 0 || observation < up) {
            throw stale(STATUS_ORDER + " 에서 " + code + " 가 " + Status.UP.getCode() + " 보다 뒤가 아니다."
                + " 관측 풀 장애가 합산 상태를 끌어내려 인스턴스가 로드밸런서에서 빠진다."
                + " (현재 값: " + order + ")");
        }

        requireMoreSevereThanUp(order, Status.DOWN.getCode(), up);
        requireMoreSevereThanUp(order, Status.OUT_OF_SERVICE.getCode(), up);
    }

    /** 빠져도, 뒤에 있어도 결과는 같다 — 진짜 장애가 UP 에 가려 200 으로 나간다. */
    private static void requireMoreSevereThanUp(List<String> order, String failure, int up) {
        int index = order.indexOf(failure);
        if (index < 0 || index > up) {
            throw stale(STATUS_ORDER + " 에서 " + failure + " 가 " + Status.UP.getCode() + " 보다 앞이 아니다"
                + (index < 0 ? "(목록에 없다 — 없으면 가장 안 심각으로 취급된다)" : "")
                + ". 운영 장애가 합산 상태에 묻혀 헬스체크가 200 을 돌려준다. (현재 값: " + order + ")");
        }
    }

    /** 이게 없으면 관측 풀 장애를 볼 창구가 사라진다 — 합산은 UP 이므로 어디에도 안 드러난다. */
    private static void requireGroupMembership(Binder binder) {
        List<String> include = binder.bind(GROUP_INCLUDE, Bindable.listOf(String.class)).orElseGet(List::of);
        if (!include.contains(ObservationHealthConfig.CONTRIBUTOR_ID)) {
            throw stale(GROUP_INCLUDE + " 가 " + ObservationHealthConfig.CONTRIBUTOR_ID + " 를 포함하지 않는다."
                + " 관측 풀 장애를 볼 창구가 없다. (현재 값: " + include + ")");
        }
    }

    /**
     * 창구가 있어도 늘 200 을 돌려주면 경보를 걸 수 없다. 기본 매핑에는 이 상태가 없어서
     * <b>적지 않으면 200 이 된다</b> — 빠뜨리기 가장 쉬운 자리다.
     */
    private static void requireGroupReportsFailure(Binder binder, String code) {
        // 키를 프로퍼티 이름으로 직접 붙이면 안 된다 — OBSERVATION_DOWN 은 대문자·밑줄이라
        // ConfigurationPropertyName 규칙에 맞지 않아 InvalidConfigurationPropertyNameException 이 난다.
        // 맵으로 바인딩하면 상태 코드가 이름이 아니라 값이 되어 원문 그대로 들어온다.
        Map<String, Integer> mapping = binder.bind(GROUP_MAPPING, Bindable.mapOf(String.class, Integer.class))
            .orElseGet(Map::of);
        Integer status = mapping.get(code);
        if (status == null || status < FAILURE_CODE_FLOOR) {
            throw stale(GROUP_MAPPING + " 의 " + code + " 가 실패 코드가 아니다(현재 값: " + status + ")."
                + " 관측 풀이 죽어도 그룹이 200 을 돌려줘 경보가 울리지 않는다.");
        }
    }

    private static IllegalStateException stale(String detail) {
        return new IllegalStateException(
            detail + " management.yml 을 management.yml.example 로 다시 복사한다.");
    }
}
