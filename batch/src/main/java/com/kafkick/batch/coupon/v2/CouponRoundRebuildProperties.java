package com.kafkick.batch.coupon.v2;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 재구성 설정.
 *
 * @param drain 게이트를 닫은 뒤 <b>진행 중인 쓰기가 빠지기를 기다리는 시간</b>.
 *     <p>게이트를 닫는 것으로 그 회차의 쓰기가 멎지는 않는다. 닫기 직전에 Redis 선점을 끝낸
 *     발급의 {@code issuances} 커밋은 그 뒤에 도착하고(발급은 선점 → DB 커밋 → 완료 순서다),
 *     취소의 Redis {@code INCR} 은 DB 커밋보다 늦다({@code afterCommit}). 둘 다 집계를 읽기
 *     전에 빠져야 재구성이 그것들을 <b>세거나 무시하거나</b> 를 일관되게 정할 수 있다.
 *     <p>그래서 값의 기준은 <b>발급 트랜잭션 시간과 {@code afterCommit} 지연의 상한</b>이다.
 *     짧으면 창이 남고, 길면 그만큼 그 회차가 오래 503 이다. 기본 2초는 그 둘 사이의 값이지
 *     측정된 상한이 아니다 — 부하 프로파일이 바뀌면 여기를 다시 봐야 한다.
 *     <p>{@code 0} 이면 기다리지 않는다. 테스트 말고는 쓰지 마라.
 */
@ConfigurationProperties(prefix = "coupon.rebuild")
public record CouponRoundRebuildProperties(Duration drain) {

    /**
     * 대기의 상한.
     *
     * <p><b>기다리는 동안 그 회차는 전면 503 이고, 회차 가드와 워커 스레드가 잡혀 있다.</b>
     * 그래서 이 값은 "길수록 안전" 이 아니다 — 오래 기다릴수록 재구성이 고치려던 장애가
     * 길어진다. {@code 2s} 를 {@code 2m} 로 잘못 적는 것이 이 설정에서 가장 흔한 오타이고,
     * 그 결과는 조용하다(기동은 성공하고 재구성만 2분씩 걸린다).
     *
     * <p>30초는 발급 트랜잭션과 {@code afterCommit} 지연의 어떤 상한보다도 크고, 사람이
     * "멈췄나?" 하고 다시 누르기 전의 시간이다. 이보다 더 기다려야 한다면 그건 대기로 풀 문제가
     * 아니라 그 회차의 발급 경로가 이미 병든 것이다.
     */
    public static final Duration MAX_DRAIN = Duration.ofSeconds(30);

    public CouponRoundRebuildProperties {
        if (drain == null || drain.isNegative()) {
            throw new IllegalArgumentException("재구성 drain 대기는 음수일 수 없습니다: " + drain);
        }
        if (drain.compareTo(MAX_DRAIN) > 0) {
            throw new IllegalArgumentException(
                    "재구성 drain 대기는 " + MAX_DRAIN + " 를 넘을 수 없습니다: " + drain
                            + " (기다리는 동안 그 회차는 전면 503 이다)");
        }
    }
}
