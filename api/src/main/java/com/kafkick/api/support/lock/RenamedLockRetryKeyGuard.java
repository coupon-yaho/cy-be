package com.kafkick.api.support.lock;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * 옛 설정 키가 남아 있으면 <b>기동을 멈춘다.</b>
 *
 * <h2>왜 필요한가</h2>
 *
 * <p>{@code coupon.issue.lock-retry} 가 {@code coupon.lock-retry} 로 바뀌었다. 그런데
 * 실제 {@code application.yml} 은 <b>커밋되지 않는다</b>({@code .gitignore}) — 각자
 * {@code .example} 을 복사해 쓰고, README 는 <i>"각자 만든 실제 파일이 있으면 그쪽이
 * 이긴다"</i> 고 적는다. 그래서 이 브랜치를 받아도 <b>이미 있는 설정 파일은 안 바뀐다.</b>
 *
 * <p>{@link LockRetryProperties} 는 값이 없으면 예외가 아니라 <b>기본값</b>을 쓴다.
 * 가드가 없으면 부하 회차에 맞춰 조율해 둔 값이 <b>아무 신호 없이</b> 3회·500ms 로
 * 되돌아간다. 기동도 성공하고 로그도 안 남아, {@code exhausted} 가 튄 뒤에야 드러난다.
 *
 * <h2>왜 빈이 아닌가</h2>
 *
 * <p>{@code DeployedConfigGuard} 가 같은 이유로 {@code EnvironmentPostProcessor} 다 —
 * 빈으로 두면 {@code @ConfigurationProperties} 바인딩이 먼저 돌아 가드가 늦는다.
 * 등록 키도 그쪽과 같이 {@code org.springframework.boot.EnvironmentPostProcessor} 다
 * (Boot 4 에서 옮겨졌고, 옛 키로 적으면 오류 없이 그냥 안 불린다).
 *
 * <p><b>고쳐 주지 않고 멈춘다.</b> 옛 값을 새 키로 옮겨 주면 조용히 동작해서, 다음 사람이
 * 옛 이름을 계속 쓰게 된다. 한 번 멈추고 사람이 옮기는 편이 낫다.
 */
public class RenamedLockRetryKeyGuard implements EnvironmentPostProcessor, Ordered {

    /** 옛 prefix. 하위 키가 하나라도 있으면 거절한다. */
    static final String LEGACY_PREFIX = "coupon.issue.lock-retry";

    static final String[] LEGACY_KEYS = {
            LEGACY_PREFIX + ".max-attempts",
            LEGACY_PREFIX + ".budget"
    };

    @Override
    public void postProcessEnvironment(
            ConfigurableEnvironment environment, SpringApplication application) {
        for (String legacyKey : LEGACY_KEYS) {
            if (environment.containsProperty(legacyKey)) {
                throw new IllegalStateException(
                        "%s 는 coupon.lock-retry 로 바뀌었습니다. 발급 전용이 아니라 사용·"
                                .formatted(LEGACY_PREFIX)
                                + "사용취소·발급취소까지 함께 쓰는 값입니다. 설정을 옮기고 "
                                + "다시 띄우세요. (환경변수도 COUPON_LOCK_MAX_ATTEMPTS · "
                                + "COUPON_LOCK_RETRY_BUDGET 로 바뀌었습니다)");
            }
        }
    }

    /**
     * {@code DeployedConfigGuard} 보다 <b>뒤에</b> 둔다. 배포 설정이 아예 안 올라온 경우가
     * 더 근본이라 그 메시지가 먼저 나와야 한다.
     */
    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
