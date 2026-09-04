package com.kafkick.infra.mq.notification;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("kafka.notification.relay")
public class NotificationRelayProperties {

    /**
     * <b>상한을 여기서 막는다 — 어댑터가 아니라.</b> 저장소의 지연 변환기는 365일을 넘으면
     * 던지는데, 그것은 <b>첫 실패가 실제로 났을 때</b> 터진다. 설정이 틀린 사실을 운영 중
     * 첫 장애 때 알게 되는 셈이라, 기동 시점으로 당긴다.
     */
    private static final Duration MAX_BACKOFF = Duration.ofDays(365);

    /**
     * 한 회차에 집을 수 있는 절대 상한.
     *
     * <p><b>메모리와 SQL 크기 때문이다.</b> 집은 수만큼 id·UUID 를 메모리에 만들고 같은 수의
     * {@code IN} 자리표시자를 붙인다. 백로그가 큰 상태에서 큰 값을 주면 릴레이가 그것 때문에 죽는다.
     *
     * <p>1,000 은 그 선을 넉넉히 아래로 잡은 값이다.
     *
     * <p><b>lease 관계 검사와 어느 쪽이 먼저 걸리는지는 lease 에 달렸다.</b>
     * 기본 lease(30초)에서는 배치 300 부터 그쪽이 먼저 막지만, lease 가 100초를 넘으면
     * 1,000 도 그 검사를 통과하므로 이 상한이 유일한 방어선이 된다. 한때 여기에
     * <i>"실제로는 lease 관계가 훨씬 먼저 걸린다"</i> 고 적었는데 lease 를 상수로 가정한 것이었다.
     */
    private static final int MAX_CLAIM_BATCH_SIZE = 1_000;

    private Duration lease = Duration.ofSeconds(30);
    private long fixedDelayMs = 100L;

    /**
     * 한 회차에 집을 최대 건수.
     *
     * <p><b>64 는 처리량이 아니라 락 보유 구간에서 나온 값이다.</b> 선점은 한 트랜잭션이고,
     * 그 안에서 잠근 행은 커밋까지 잡혀 있다. 크게 잡으면 왕복은 줄지만 그만큼 오래 잡는다.
     *
     * <p>{@code SKIP LOCKED} 가 다른 워커를 기다리게 하지는 않으므로 <b>경합이 아니라
     * 회복 지연</b>이 비용이다 — 이 배치가 도는 동안 죽으면 그만큼이 lease 만료를 기다린다.
     */
    private int claimBatchSize = 64;

    public int getClaimBatchSize() {
        return claimBatchSize;
    }


    /**
     * @throws IllegalArgumentException 1 미만이거나 {@value #MAX_CLAIM_BATCH_SIZE} 초과일 때.
     *         0 이면 릴레이가 아무것도 집지 않고 <b>조용히 정상으로 보이고</b>,
     *         너무 크면 메모리와 SQL 크기로 릴레이가 죽는다
     */
    public void setClaimBatchSize(int claimBatchSize) {
        if (claimBatchSize < 1 || claimBatchSize > MAX_CLAIM_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "claim batch size 는 1 이상 " + MAX_CLAIM_BATCH_SIZE + " 이하여야 합니다. "
                            + "받은 값=" + claimBatchSize);
        }
        this.claimBatchSize = claimBatchSize;
    }

    /**
     * Full Jitter 의 기본 간격. 첫 재시도 상한이 {@code base × 2} 다.
     *
     * <p>200ms 인 이유 — 이보다 짧으면 실패한 것들이 사실상 즉시 다시 와 지터가 무의미하고,
     * 길면 정상 회복이 느려진다. 상한({@link #backoffCap})이 폭주를 막으므로 여기는 짧게 둔다.
     */
    private Duration backoffBase = Duration.ofMillis(200);

    /**
     * Full Jitter 의 지연 상한.
     *
     * <p><b>이 값이 사는 대가는 회복이 느려지는 것이다.</b> {@code failure_count} 상한이
     * 10 이므로 계속 실패하는 명령이 {@code DEAD} 에 닿는 시간이 함께 늘어난다.
     *
     * <pre>
     *   attempt별 상한(ms)  400 · 800 · 1,600 · 3,200 · 6,400 · 12,800 · 20,000 · 20,000 · 20,000
     *                       (200 &lt;&lt; 7 = 25,600 이라 7회차부터 cap 에 걸린다)
     *
     *   실제로 기다리는 것은 아홉 번이다 — 열 번째 실패는 {@code failure_count} 를 10 으로
     *   올려 그 자리에서 {@code DEAD} 로 보내므로 그 지연은 쓰이지 않는다.
     *
     *   최악 85.2초 · 기대 42.6초      (고정 1초였을 때는 최악 9초)
     * </pre>
     *
     * <p><b>{@code 10 × cap = 200초} 가 아니다.</b> 앞쪽 회차의 상한이 작기 때문이고,
     * 한때 그렇게 적었다가 Qodo 리뷰가 잡았다.
     *
     * <p><b>그 교환을 받아들이는 이유</b> — 이 저장소의 알림에는 "언제까지 종착 상태여야
     * 한다" 는 마감이 없다. 늦게 가는 것보다 <b>한꺼번에 몰려 다시 실패하는 것</b>이 나쁘다.
     * 마감이 있는 쪽(예: 소비자에게 SLA 가 붙은 발행)에 이 백오프를 쓰게 되면
     * {@code 10 × cap < 마감} 을 기동 시 검증해야 한다 — 안 그러면 아직 재시도 중인 것을
     * 마감 쪽이 먼저 실패로 닫고, 뒤늦게 성공한 결과가 갈 곳을 잃는다.
     */
    private Duration backoffCap = Duration.ofSeconds(20);

    public Duration getBackoffBase() {
        return backoffBase;
    }

    /**
     * @throws IllegalArgumentException {@code null}·0·음수이거나 365일을 넘을 때.
     *         {@code @ConfigurationProperties} 는 setter 로 바인딩하므로 이 예외는
     *         <b>기동 중 빈 생성에서</b> 터진다 — 그것이 이 검사를 여기 둔 이유다
     */
    public void setBackoffBase(Duration backoffBase) {
        this.backoffBase = requirePositive(backoffBase, "backoff base");
    }

    public Duration getBackoffCap() {
        return backoffCap;
    }

    /**
     * @throws IllegalArgumentException {@code null}·0·음수이거나 365일을 넘을 때.
     *         {@link #setBackoffBase} 와 같은 시점에 터진다
     */
    public void setBackoffCap(Duration backoffCap) {
        this.backoffCap = requirePositive(backoffCap, "backoff cap");
    }

    private static Duration requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + "는 양수여야 합니다.");
        }
        if (value.compareTo(MAX_BACKOFF) > 0) {
            throw new IllegalArgumentException(
                    name + "는 365일 이하여야 합니다. 저장소의 지연 변환기가 그 위에서 던지는데, "
                            + "그것은 첫 실패가 났을 때야 터집니다. 받은 값=" + value);
        }
        return value;
    }

    public Duration getLease() {
        return lease;
    }

    public void setLease(Duration lease) {
        if (lease == null || lease.isZero() || lease.isNegative()) {
            throw new IllegalArgumentException("relay lease는 양수여야 합니다.");
        }
        this.lease = lease;
    }

    public long getFixedDelayMs() {
        return fixedDelayMs;
    }

    public void setFixedDelayMs(long fixedDelayMs) {
        if (fixedDelayMs < 1) {
            throw new IllegalArgumentException("relay fixed delay는 양수여야 합니다.");
        }
        this.fixedDelayMs = fixedDelayMs;
    }
}
