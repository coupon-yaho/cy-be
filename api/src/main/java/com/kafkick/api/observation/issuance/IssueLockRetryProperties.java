package com.kafkick.api.observation.issuance;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 발급이 락 경합으로 물러설 때의 재시도 한도.
 *
 * <p><b>상수로 두지 않는 이유</b> — 부하 회차 중에 재기동 없이 만져야 한다. 데드락이
 * 예상보다 잦아 상한을 올리고 싶을 때 재빌드·재배포가 필요하면 측정 구간이 끊긴다.
 *
 * @param maxAttempts 총 시도 횟수. <b>작게 둔다</b> — 데드락은 한쪽이 이미 롤백된 상태라
 *                    대개 다음 시도에서 풀린다
 * @param budget      재시도에 쓸 수 있는 시간. <b>횟수만으로는 부족하다</b> — 스프링은
 *                    데드락(1213)과 <b>락 대기 타임아웃(1205)</b>을 같은 예외로 번역한다.
 *                    데드락은 밀리초 안에 실패해 예산에 다시 들어오지만, 락 대기 타임아웃은
 *                    {@code innodb_lock_wait_timeout}(기본 50초)을 다 쓰고 온다.
 *                    횟수만 두면 그 한 요청이 커넥션을 150초 물게 된다 — 첫 실패가 이미
 *                    예산을 넘겼으면 다시 시도하지 않는다
 */
@ConfigurationProperties(prefix = "coupon.issue.lock-retry")
public record IssueLockRetryProperties(
        Integer maxAttempts,
        Duration budget
) {

    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final Duration DEFAULT_BUDGET = Duration.ofMillis(500);

    public IssueLockRetryProperties {
        maxAttempts = maxAttempts == null ? DEFAULT_MAX_ATTEMPTS : maxAttempts;
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts는 1 이상이어야 합니다.");
        }
        budget = budget == null ? DEFAULT_BUDGET : budget;
        if (budget.isNegative() || budget.isZero()) {
            throw new IllegalArgumentException("budget은 양수여야 합니다.");
        }
    }

    /** 기본값 그대로. 테스트가 값을 안 바꿀 때 쓴다. */
    public static IssueLockRetryProperties defaults() {
        return new IssueLockRetryProperties(null, null);
    }
}
