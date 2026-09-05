package com.kafkick.infra.mq.notification;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("kafka.notification.relay")
public class NotificationRelayProperties {


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

    /**
     * 워커 수의 절대 상한.
     *
     * <p><b>스레드마다 스택이 붙는다.</b> 이 풀은 접수 API 프로세스 안에서 도는데, 거기에는
     * 요청 처리 스레드가 이미 있다. 릴레이가 그것을 밀어내면 <b>알림을 빨리 보내려다 접수를
     * 느리게 만든다</b> — 순서가 뒤집힌다.
     *
     * <p>{@link #maxInFlight} 를 넘겨 봤자 일하지 않는 스레드만 는다.
     */
    private static final int MAX_WORKER_COUNT = MAX_CLAIM_BATCH_SIZE;

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

    /**
     * 동시에 워커 풀에 물려 둘 최대 건수. <b>백프레셔의 기준선이다.</b>
     *
     * <p>{@link #claimBatchSize} 와 같은 64 다. 더 크게 잡을 이유가 없다 — 한 회차에 집는
     * 것이 그 이하이므로, 이 값을 키워 봤자 <b>여러 회차의 인플라이트가 겹칠 수 있는 폭</b>만
     * 넓어지고 그만큼 lease 검사가 빡빡해진다.
     *
     * <p>반대로 이보다 작게 잡으면 백프레셔가 배치를 잘라 <b>집는 수 자체가 줄어든다.</b>
     * 그것이 필요한 상황(발행 대상이 느려 인플라이트를 좁게 쥐고 싶을 때)이 있으므로 막지 않는다.
     */
    private int maxInFlight = 64;

    /**
     * 발행을 실제로 수행하는 스레드 수.
     *
     * <h2>실측 — 배치 클레임만으로는 처리량이 안 는다</h2>
     *
     * <p>실제 MySQL 에 대기 500건을 심고, 건당 20ms 걸리는 발행 대상으로 다 빼는 데 걸린
     * 시간을 쟀다(스케줄러 간격 없이 연속 폴링).
     *
     * <pre>
     *   워커 1 · 배치 1   20,297ms   24.6건/s   ← CY-902 이전
     *   워커 1 · 배치 64  15,575ms   32.1건/s   ← 배치 클레임만: +30%
     *   워커 8 · 배치 64   1,682ms  297.3건/s   ← 워커 풀까지: 12배
     *   워커 16 · 배치 64    842ms  593.8건/s
     * </pre>
     *
     * <p><b>이 표가 말하는 것과 말하지 않는 것을 갈라 둔다.</b> 말하는 것은 <b>배치 클레임
     * 하나로는 30% 밖에 못 얻는다</b>는 것이다 — 집는 것을 넓혀도 <b>보내는 것이 차례면</b>
     * 거기서 막힌다. 그래서 CY-902 와 이 티켓이 짝이다.
     *
     * <h2>실측 — 진짜 Kafka 로 다시 쟀다 (CY-922)</h2>
     *
     * <p>위 표는 발행 대상이 {@code sleep(20ms)} 스텁이라 <b>워커를 몇으로 둘지는 말하지
     * 못했다.</b> 실제 MySQL + 실제 Kafka(운영과 같은 프로듀서 설정)로 5,000건을 빼며,
     * <b>같은 JVM · 같은 커넥션 풀</b>의 요청 스레드 지연을 함께 쟀다(두 회차).
     *
     * <pre>
     *   워커  1    98 ~ 102건/s     요청 p99  1.9 ~ 2.1ms
     *   워커  2   203 ~ 247건/s     요청 p99  0.6 ~ 1.3ms
     *   워커  4   400 ~ 459건/s     요청 p99  0.6 ~ 1.1ms
     *   워커  8   582 ~ 704건/s     요청 p99  1.9 ~ 2.6ms   ← 기본값
     *   워커 16  1098 ~ 1122건/s    요청 p99 10.4 ~ 12.8ms
     *   워커 32  1439 ~ 1496건/s    요청 p99 18.4 ~ 19.4ms
     * </pre>
     *
     * <p><b>처리량은 32 까지도 안 평평해진다.</b> CY-906 이 <i>"진짜 Kafka 에서는 브로커
     * 왕복이 먼저 평평해진다"</i> 고 적어 뒀는데 <b>그 예상이 빗나갔다</b> — 먼저 무너진
     * 것은 브로커가 아니라 <b>같은 JVM 의 요청 스레드</b>다.
     *
     * <p><b>요청 지연은 8 까지 평평하다.</b> 워커 1 과 워커 8 의 p99 가 같은 수준이고,
     * 16 에서 다섯 배가 된다. 그래서 8 → 16 은 <b>처리량 ×1.6 을 사려고 요청 p99 ×5 를
     * 내는</b> 거래다. <b>8 은 이제 비용에서 고른 값이 아니라 잰 값이다.</b>
     *
     * <p>⚠️ <b>무너지는 자리는 브로커가 아니라 코어 수다.</b> 잰 기계는
     * {@code availableProcessors = 11} 이라 워커 8 + 스케줄러면 아직 안 넘고 16 은 넘는다.
     * <b>코어가 4 인 기계에서는 8 도 이미 초과 예약이다</b> — 이 값을 다른 배포 대상에
     * 그대로 옮기지 말 것. 옮기려면 그 기계에서 다시 재야 한다.
     *
     * <p>전체 표·재현 절차·<b>세 번 헛잰 기록</b>은
     * {@code docs/18-relay-throughput-measurement.md} 에 있다.
     *
     * <p><b>이 값과 {@link #maxInFlight} 의 비가 lease 검사를 정한다</b> —
     * {@code ceil(maxInFlight / workerCount)} 가 파도 깊이이고, 릴레이 생성자가
     * 그 깊이 × 건당 예산을 lease 와 견준다. 워커를 줄이면 그 검사가 먼저 막는다.
     */
    private int workerCount = 8;

    public int getClaimBatchSize() {
        return claimBatchSize;
    }

    public int getMaxInFlight() {
        return maxInFlight;
    }

    /**
     * @throws IllegalArgumentException 1 미만이거나 {@value #MAX_CLAIM_BATCH_SIZE} 초과일 때.
     *         0 이면 백프레셔가 항상 걸려 릴레이가 <b>아무것도 집지 않고 조용히 정상으로
     *         보인다.</b> 상한을 배치와 같이 두는 이유는 인플라이트마다 클레임 한 건이
     *         메모리에 남기 때문이다
     */
    public void setMaxInFlight(int maxInFlight) {
        if (maxInFlight < 1 || maxInFlight > MAX_CLAIM_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "max in-flight 는 1 이상 " + MAX_CLAIM_BATCH_SIZE + " 이하여야 합니다. "
                            + "받은 값=" + maxInFlight);
        }
        this.maxInFlight = maxInFlight;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    /**
     * @throws IllegalArgumentException 1 미만이거나 {@value #MAX_WORKER_COUNT} 초과일 때.
     *         0 이면 풀이 아무것도 실행하지 못해 인플라이트가 상한에 붙은 채 굳는다
     */
    public void setWorkerCount(int workerCount) {
        if (workerCount < 1 || workerCount > MAX_WORKER_COUNT) {
            throw new IllegalArgumentException(
                    "worker count 는 1 이상 " + MAX_WORKER_COUNT + " 이하여야 합니다. "
                            + "받은 값=" + workerCount);
        }
        this.workerCount = workerCount;
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
