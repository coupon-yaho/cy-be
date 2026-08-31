package com.kafkick.api.observation.issuance;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 발급이 락 경합으로 물러설 때의 재시도 한도.
 *
 * <p><b>상수로 두지 않는 이유</b> — 데드락이 예상보다 잦아 상한을 올리고 싶을 때 코드를
 * 고치고 이미지를 다시 굽지 않아도 되게 한다.
 *
 * <p>⚠️ <b>다만 재기동 없이 바뀌지는 않는다.</b> 이 record 는 기동 때 한 번 바인딩되어
 * 생성자로 주입되고, 이 저장소에는 {@code @RefreshScope} 같은 런타임 재바인딩 장치가 없다.
 * 값을 바꾸려면 <b>환경변수를 고치고 다시 띄워야</b> 한다 — 이미지는 그대로다.
 *
 * @param maxAttempts 총 시도 횟수. <b>작게 둔다</b> — 데드락은 한쪽이 이미 롤백된 상태라
 *                    대개 다음 시도에서 풀린다
 * @param budget      재시도에 쓸 수 있는 시간. <b>횟수만으로는 부족하다</b> — 스프링은
 *                    데드락(1213)과 <b>락 대기 타임아웃(1205)</b>을 같은 예외로 번역한다.
 *                    데드락은 밀리초 안에 실패해 예산에 다시 들어오지만, 락 대기 타임아웃은
 *                    {@code innodb_lock_wait_timeout}(기본 50초)을 다 쓰고 온다.
 *                    횟수만 두면 그 한 요청이 커넥션을 150초 물게 된다 — 첫 실패가 이미
 *                    예산을 넘겼으면 다시 시도하지 않는다
 *
 * @throws IllegalArgumentException {@code maxAttempts} 가 1 미만이거나 {@link #MAX_ATTEMPTS_LIMIT}
 *         초과일 때, 또는 {@code budget} 이 0 이하이거나 {@link #BUDGET_LIMIT} 초과일 때.
 *         <b>바인딩에서 던지므로 기동이 중단된다</b> — 잘못된 값으로 뜬 채 부하를 받는 것보다
 *         그 자리에서 이름을 대고 죽는 편이 낫다
 */
@ConfigurationProperties(prefix = "coupon.issue.lock-retry")
public record IssueLockRetryProperties(
        Integer maxAttempts,
        Duration budget
) {

    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final Duration DEFAULT_BUDGET = Duration.ofMillis(500);

    /** 이 이상은 재시도가 아니라 장애를 덮는 것이다. */
    static final int MAX_ATTEMPTS_LIMIT = 10;

    /**
     * 예산 상한. <b>범위를 안 막으면 요청 경로에서 터진다</b> — {@code Duration.toNanos()} 는
     * 나노초 범위를 넘는 값에서 {@code ArithmeticException} 을 내고, 범위 안이라도 아주 큰
     * 값은 마감 시각 덧셈을 넘치게 해 <b>즉시 만료</b>처럼 동작한다. 발급 한 건을 10초 넘게
     * 붙잡을 이유도 없다.
     */
    static final Duration BUDGET_LIMIT = Duration.ofSeconds(10);

    public IssueLockRetryProperties {
        maxAttempts = maxAttempts == null ? DEFAULT_MAX_ATTEMPTS : maxAttempts;
        if (maxAttempts < 1 || maxAttempts > MAX_ATTEMPTS_LIMIT) {
            throw new IllegalArgumentException(
                    "maxAttempts는 1 이상 " + MAX_ATTEMPTS_LIMIT + " 이하여야 합니다: " + maxAttempts);
        }
        budget = budget == null ? DEFAULT_BUDGET : budget;
        if (budget.isNegative() || budget.isZero() || budget.compareTo(BUDGET_LIMIT) > 0) {
            throw new IllegalArgumentException(
                    "budget은 양수이고 " + BUDGET_LIMIT + " 이하여야 합니다: " + budget);
        }
    }

    /** 기본값 그대로. 테스트가 값을 안 바꿀 때 쓴다. */
    public static IssueLockRetryProperties defaults() {
        return new IssueLockRetryProperties(null, null);
    }
}
