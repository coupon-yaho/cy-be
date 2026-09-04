package com.kafkick.core.notification.retry;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <b>재시도 지연 정책을 한 벌로 세운다.</b>
 *
 * <h2>왜 {@code core} 인가 — 두 경로가 서로 다른 모듈에 있다</h2>
 *
 * <p>알림 발행 명령이 다시 집히는 경로는 둘이다.
 *
 * <ul>
 *   <li><b>발행 실패</b> — {@code infra:mq} 의 릴레이가 {@code markFailed} 로 되돌린다</li>
 *   <li><b>lease 만료 회수</b> — {@code storage} 의 어댑터가 {@code claimBatch} 안에서
 *       되돌린다</li>
 * </ul>
 *
 * <p><b>두 모듈은 서로를 못 본다.</b> {@code infra:mq} 와 {@code storage} 는 의존 관계가
 * 없고 둘 다 {@code core} 만 본다. 그래서 정책이 한 벌이려면 그 한 벌이 여기 있어야 한다.
 *
 * <h2>후보 셋 중 무엇을 골랐나 (CY-907, #196)</h2>
 *
 * <p>티켓이 셋을 놓고 결정을 요구했다.
 *
 * <ol>
 *   <li><b>{@code core} 에 포트를 두고 어댑터에 주입</b> — <b>이것을 골랐다</b>(변형).
 *       인터페이스는 안 뒀다. 구현이 하나뿐이고 릴레이 테스트도 진짜 계산을 써야
 *       의미가 있어서, 포트를 두면 <b>가짜 이름 하나가 늘 뿐</b>이다. 소유는 여전히
 *       정책 쪽이고 어댑터는 <b>주입받아 쓰기만</b> 하므로, CY-903 이 세운
 *       <i>"지연 정책은 어댑터가 정하지 않는다"</i> 는 그대로다.</li>
 *   <li><b>회수 결과를 릴레이에 돌려주고 릴레이가 지연을 정한다</b> — <b>안 골랐다.</b>
 *       회수는 {@code claimBatch} <b>안에서</b> 일어난다. 지연을 나중에 정하려면
 *       되돌리는 쓰기를 두 번으로 쪼개야 하는데, 그 사이에 죽으면 행이 어중간한 상태로
 *       남는다. <b>왕복 하나 늘리는 문제가 아니라 원자성을 깨는 문제였다.</b></li>
 *   <li><b>어댑터 안에 계산을 두 벌</b> — 티켓이 금지했고 동의한다. 두 경로가 갈리는 날
 *       어느 쪽이 진짜인지 알 수 없다.</li>
 * </ol>
 *
 * <h2>{@code kafka.notification.relay} 아래에서 꺼낸 이유</h2>
 *
 * <p>예전 키는 {@code kafka.notification.relay.backoff-base/cap} 이었다. 그 이름을 두면
 * <b>storage 가 kafka 설정을 읽는 모양</b>이 되고, 무엇보다 그 속성 클래스는
 * {@code infra:mq} 에 있어 {@code storage} 가 볼 수 없다. 그래서
 * {@code notification.outbox.retry.*} 로 옮겼다. 바깥으로 드러나는 환경변수 이름도 같이
 * 바꿨는데, {@code .env.example} 에 없는 이름이라 <b>실제로 그것을 설정하는 배포가
 * 없다</b>(실측) — 지금이 아니면 못 바꾼다.
 */
@Configuration
@EnableConfigurationProperties(NotificationRetryBackOffProperties.class)
public class NotificationRetryBackOffConfig {

    @Bean
    public FullJitterBackOff notificationRetryBackOff(NotificationRetryBackOffProperties props) {
        return new FullJitterBackOff(props.getBase(), props.getCap());
    }
}
