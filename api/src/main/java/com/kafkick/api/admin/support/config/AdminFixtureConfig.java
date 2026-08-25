package com.kafkick.api.admin.support.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.admin.issuancehistory.mock.AdminIssuanceHistoryMockDataFactory;

/**
 * <b>[OBS-36] 관리자 화면 fixture 를 등록할지 말지를 API 가 소유한다.</b>
 *
 * <p>남은 Factory는 예전에 {@code core}에서 조건 없는 {@code @Component}였다. 그래서
 * <b>운영에서도 fixture 가 200 으로 나갔고</b>, 관리자 화면과 아무 상관 없는 배치 프로세스까지
 * 매 기동마다 그 빈을 만들었다. 관측 계정에서 {@code members} 를 걷어내는 것과 같은 축이라
 * 함께 막는다 — 운영에 남으면 안 되는 것이 조용히 남지 않게 한다.
 *
 * <p><b>조건을 {@code core} 가 아니라 여기에 두는 이유.</b> 이 저장소는 "core 는 기술 중립이고
 * 배선은 바깥 계층이 소유한다" 를 규약으로 갖는다 — {@code AdminOverviewService} javadoc 이
 * "API 설정이 실제 관측 원천과 함께 명시적으로 bean 을 소유한다" 고 못박고 있고,
 * {@code AdminAnalyticsConfig} 가 같은 형태다. 조건을 {@code core} 에 박으면 그 모듈을 쓰는
 * 모든 애플리케이션이 이 프로퍼티 이름과 형태를 강제로 따라야 한다.
 *
 * <p><b>덤으로 사라지는 함정</b> — 클래스 레벨 {@code @ConditionalOnProperty} 는
 * {@code ApplicationContextRunner} 의 명시적 {@code withBean}·{@code withUserConfiguration}
 * 등록까지 걸러낸다(실측). 조건이 클래스가 아니라 {@code @Bean} 메서드에 있으면, 그 Factory 를
 * 직접 꽂는 테스트는 스위치를 몰라도 된다.
 *
 * <p><b>끄면 PENDING이 아니라 기동이 실패한다.</b> 남은 화면의 응답 계약
 * ({@code AdminIssuanceHistoryResult})에는 "아직 집계되지 않았음"
 * 을 표현할 자리가 없어서, 빈 결과를 내면 "조회했는데 없다" 와 구분되지 않는다. 그 구분을
 * 만드는 것은 응답 계약 변경이고 A-08의 몫이다 — 여기서 발명하지 않는다.
 * {@code analytics} 만 그 자리가 이미 있어서({@code AdminAnalyticsPendingSource}) 혼자
 * 다르게 동작한다.
 *
 * <p>즉 이 스위치의 대가는 이렇다 — 켜면 가짜 수치가 200 으로 나가고, 끄면 앱이 안 뜬다.
 * 둘 다 나쁘지만 <b>기본값이 어느 쪽이냐</b>가 다르다. 지금 기본값은 꺼짐이고,
 * {@code .env.example} 이 켜는 선택을 눈에 보이는 한 줄로 적어 둔다.
 */
@Configuration(proxyBeanMethods = false)
public class AdminFixtureConfig {

    /**
     * fixture 를 켜는 스위치. <b>이 상수가 정본이다</b> — 리터럴을 여기저기 옮겨 적으면
     * 한 곳만 바뀌어도 그 화면만 조용히 다르게 동작한다.
     *
     * <p>{@code .env.example} 의 {@code ADMIN_MOCK_ENABLED} 와 두 {@code application.yml.example}
     * 이 같은 이름을 가리키는지는 {@code AdminFixtureExposureTest} 가 세 파일을 대조한다.
     */
    public static final String FIXTURE_SWITCH = "admin.mock.enabled";

    @Bean
    @ConditionalOnProperty(name = FIXTURE_SWITCH, havingValue = "true")
    public AdminIssuanceHistoryMockDataFactory adminIssuanceHistoryMockDataFactory() {
        return new AdminIssuanceHistoryMockDataFactory();
    }
}
