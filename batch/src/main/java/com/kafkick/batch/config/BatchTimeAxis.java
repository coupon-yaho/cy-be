// 배치 메타 시각(JVM 기본 존)을 도메인 축(UTC)으로 옮깁니다.
package com.kafkick.batch.config;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * <b>이 저장소에는 시각 축이 둘이다.</b> 도메인 시각은 전부 {@code TimeProvider}(UTC)에서
 * 오는데({@code as_of} · {@code from_ts} · 정리 배치의 컷오프), 스프링 배치가 실행 시각을
 * 찍을 때 쓰는 것은 <b>인자 없는 {@code LocalDateTime.now()}</b> 라 그 값이 <b>JVM 기본 존</b>
 * 벽시계다({@code AbstractJob} · {@code AbstractStep}, 6.0.4 바이트코드로 확인).
 *
 * <p><b>둘을 한 행에 같이 넣거나 서로 비교하면 축이 어긋난다.</b> 예외도 로그도 안 나고
 * 판정 결과도 안 바뀌어서, <b>나중에 그 시각으로 사건을 맞춰 볼 때</b>만 드러난다 —
 * {@code DefaultZoneGuard} 가 존재하는 이유가 그 조용함이다.
 *
 * <p><b>여기서 옮긴다 — 저장 계층이 아니라.</b> 처음엔 어댑터의 바인딩을
 * {@code Timestamp.valueOf} 로 감쌌는데, 그러면 <b>저장 계층이 값의 출처를 안다고 가정</b>하게
 * 된다. 어댑터에 오는 값이 배치 메타에서 온 것인지 도메인이 계산한 것인지 구분할 수 없고,
 * 실제로 그렇게 만들었더니 <b>같은 축으로 넣은 테스트 스물한 개가 불변식에서 죽었다</b>
 * ({@code VerificationRun} 의 {@code asOf <= startedAt <= finishedAt} 은 둘이 같은 축임을
 * 전제한다). 존이 섞이는 지점에서 닫는 것이 맞다.
 *
 * <p><b>값의 뜻은 안 바뀐다.</b> 여전히 {@code JobExecution.getStartTime()} 이고
 * ({@code .coderabbit.yaml} 이 계약으로 못 박은 출처다) {@code TimeProvider.now()} 로
 * 갈아타지 않는다 — 같은 순간을 다른 존의 벽시계로 적을 뿐이다.
 *
 * <p>⚠️ <b>구조적 가드가 없다.</b> 형제인 {@code StuckRunClaim#claim} 은 바인딩을 감싸고도
 * 부족해서 {@code StuckBeforeBindingIsCentralizedTest} 로 소스를 훑는다. 여기는 그 검사가
 * 없으니, <b>배치 메타 시각을 도메인으로 넘기는 자리를 새로 만들면 반드시 이 메서드를
 * 거쳐라.</b> 안 거쳐도 배포는 {@code DefaultZoneGuard} 가 UTC 를 강제해 증상이 안 드러나고,
 * 비UTC 환경에서만 조용히 어긋난다.
 */
public final class BatchTimeAxis {

    private BatchTimeAxis() {
    }

    /** JVM 기본 존을 쓴다. 배치 메타가 그 존으로 찍기 때문이다. */
    public static LocalDateTime onDomainAxis(LocalDateTime batchMetaTime) {
        return onDomainAxis(batchMetaTime, ZoneId.systemDefault());
    }

    /**
     * <b>존을 인자로 받는 갈래.</b> 기본 존은 테스트가 못 바꾸므로, 변환 자체를 재려면 이쪽이
     * 필요하다 — 통합 테스트는 {@code build.gradle} 의 {@code user.timezone} 한 줄에 매여
     * 그 값이 UTC 가 되는 날 <b>통째로 건너뛰어진다.</b>
     *
     * <p>⚠️ <b>DST 전이가 있는 존에서는 겹침 시각에 한 시간 어긋난다.</b>
     * {@code atZone} 은 유효 오프셋이 둘일 때 <b>이른 쪽</b>을 고르는데, 배치 메타 값만으로는
     * 어느 쪽이었는지 알 수 없다({@code LocalDateTime} 에 오프셋이 없다). 그런 존은
     * {@code DefaultZoneGuard} 가 <b>고정 오프셋만 통과시켜</b> 기동을 거절하므로 배포에서는
     * 도달하지 않는다 — 가드가 꺼진 환경에서만 남는 한계다.
     *
     * <p>{@code null} 은 안 받는다. 호출부에 오는 값은 프레임워크가 태스크릿 실행 <b>전에</b>
     * 세우고, {@code null} 이면 도메인의 검증이 즉시 던진다 — 여기서 조용히 흘려 보내면
     * 실패 지점이 저장 계층으로 밀린다.
     */
    static LocalDateTime onDomainAxis(LocalDateTime batchMetaTime, ZoneId batchMetaZone) {
        return batchMetaTime.atZone(batchMetaZone)
                .withZoneSameInstant(ZoneOffset.UTC)
                .toLocalDateTime();
    }
}
