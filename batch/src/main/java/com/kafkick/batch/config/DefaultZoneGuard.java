// JVM 기본 타임존이 UTC 인지 기동 시점에 확인합니다.
package com.kafkick.batch.config;

import java.time.ZoneId;
import java.time.ZoneOffset;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * <b>배치는 시계를 둘 쓴다.</b> {@code TimeProvider}({@code Clock.systemUTC})와, 스프링 배치가
 * 메타에 찍는 <b>인자 없는 {@code LocalDateTime.now()}</b>(= JVM 기본 존)다. 판정에 쓰이는
 * 컬럼은 CY-397 이 앞쪽으로 옮겨 존과 무관해졌지만, <b>배치 메타 자신은 여전히 뒤쪽</b>이다 —
 * {@code START_TIME} 은 {@code AbstractJob} 이, {@code StepExecution.LAST_UPDATED} 는
 * {@code SimpleJobRepository.update} 가 각각 그것으로 찍는다(6.0.4 바이트코드로 확인).
 *
 * <p><b>{@link RunningJobProbe} 는 안 어긋난다.</b> 한때 이 자리에 그것을 근거로 적었는데
 * 실측에서 거짓이었다 — 쓰기와 읽기가 <b>같은 {@code java.sql.Timestamp} 축을 대칭으로</b>
 * 지나 같은 JVM 안의 왕복은 존과 무관하게 항등이다(CY-718 실측: KST 에서 {@code 16:42:55}
 * 로 심고 {@code jobRepository} 로 읽으면 그대로 {@code 16:42:55}).
 *
 * <p><b>깨지는 자리는 "원시 {@code LocalDateTime} 을 바인딩한다" 가 아니라 "자바 쪽 값이
 * JVM 기본 존이다" 로 갈린다.</b> {@link TimestampBindingAxisTest} 가 서버가 실제로 본 값을
 * 단언한다 — 원시 {@code LocalDateTime} 은 <b>그대로</b>({@code 16:42:55}) 가고,
 * {@code Timestamp.valueOf} 는 세션 존으로 정규화돼({@code 07:42:55}) 간다. 컬럼은 후자로
 * 쓰인 값이므로, <b>자바 쪽 값이 JVM 기본 존일 때만</b> 두 축이 어긋난다.
 * <ul>
 *   <li>{@code StuckRunClaim.CLAIM} · {@code VerifyStopService.CLAIM} 의
 *       {@code :stuckBefore} — 값이 배치 메타에서 온 <b>JVM 기본 존</b>이라 어긋났다.
 *       <b>동쪽 존에서만 위험했다</b>: KST 면 컷오프가 아홉 시간 앞서 진도 조건이
 *       <b>항상 참</b>이 되고, 살아 있는 실행이 시체 판정을 통과해 복구·중단 API 가
 *       <b>도는 잡을 닫았다.</b> 서쪽 존은 반대로 창이 넓어져 <b>진짜 시체도 오프셋만큼
 *       늦게</b> 걷힌다 — 덜 위험할 뿐 둘 다 틀린 답이다.
 *       CY-718 이 {@code StuckRunClaim#claim} 안에 바인딩을 가둬 축을 맞췄다
 *   <li>{@code verification_runs.started_at}·{@code finished_at} — 배치 메타의
 *       {@code getStartTime()}(JVM 기본 존)인데 <b>같은 행의</b> {@code as_of} 는
 *       {@code TimeProvider}(UTC)라 어긋났다. CY-743 이
 *       {@code BatchTimeAxis#onDomainAxis} 로 닫았다 — <b>호출부</b>에서 값을 옮기므로
 *       어댑터에는 이미 UTC 축인 값만 들어간다. 값의 뜻은 안 바꿨다
 * </ul>
 * 둘 다 <b>예외 없이 조용히</b> 틀린 답을 낸다.
 *
 * <p>⚠️ <b>{@code CleanupJobConfig} 의 메타 보존 컷오프는 여기 <u>해당하지 않는다</u>.</b>
 * 그 값은 {@code TimeProvider}(UTC)에서 오고 원시로 바인딩되니 UTC 컬럼과 <b>같은 축</b>이다 —
 * 한때 이 목록에 셋째로 적혀 있었는데 거짓이었다. 그 말을 믿고 {@code Timestamp.valueOf} 를
 * 씌우면 <b>멀쩡하던 자리를 존 오프셋만큼 밀어</b> 망가뜨린다.
 *
 * <p><b>명시적으로 박아 둔 곳은 둘뿐이다</b> — {@code batch.yml} 의 {@code TZ} 와
 * {@code batch/build.gradle} 의 {@code bootRun} {@code user.timezone}. 컨테이너가 UTC 인
 * 셋째 경로는 <b>베이스 이미지({@code eclipse-temurin})의 기본값이 우연히 그런 것</b>이지
 * {@code Dockerfile} 이 박은 것이 아니다 — 베이스를 바꾸는 날 조용히 흔들린다.
 * 어느 경로가 빠져도 기동은 성공하고 증상은 <b>가드가 안 우는 것</b>으로만 나타난다 —
 * 조용한 실패라 여기서 잡는다.
 *
 * <p><b>테스트 JVM 은 일부러 {@code Asia/Seoul} 이다</b>({@code batch/build.gradle} 의
 * {@code test} 태스크). {@code TimestampMappingTest} 가 그 존에서 드라이버가 시각을 안 미는
 * 것을 단언하기 때문이다. 그래서 이 가드는 <b>끌 수 있어야 하고</b>, 테스트 설정
 * ({@code batch/src/test/resources/application.yml})이 한 곳에서 끈다 —
 * {@code VerifyJobConfig} 가 <i>"그 KST 테스트와 부딪혀 docs/13 으로 미뤘다"</i> 고 적은
 * 자리가 이것이고, 그 충돌을 스위치로 갈랐다.
 */
@Component
public class DefaultZoneGuard {

    private static final Logger log = LoggerFactory.getLogger(DefaultZoneGuard.class);

    /** 거절을 끄는 손잡이. 기본은 켬 — 끄는 법은 거절 메시지가 직접 말한다. */
    static final String REQUIRED = "batch.timezone-guard.required";

    public DefaultZoneGuard(@Value("${" + REQUIRED + ":true}") boolean required,
            MeterRegistry registry) {
        ZoneId zone = ZoneId.systemDefault();
        publish(registry, required, zone);
        if (isUtc(zone)) {
            log.info("JVM 기본 존 확인 완료 — {} 로 배치 메타와 같은 좌표계입니다.", zone);
            return;
        }
        // **끄는 법은 거절할 때만 말한다.** 이미 끈 상태의 로그에 그 안내를 또 실으면
        // 운영자가 "이미 한 조치를 또 하라" 로 읽는다 — 형제 둘도 그렇게 갈라 뒀다.
        String message = "JVM 기본 존이 UTC 가 아닙니다: " + zone + ". "
                + "스프링 배치는 배치 메타 시각을 Timestamp.valueOf 로 써서 세션 존(UTC)"
                + "으로 정규화하는데, SQL 에 원시 LocalDateTime 으로 바인딩하는 값은 그 "
                + "정규화를 안 탑니다 — 두 축이 이 존의 오프셋만큼 어긋납니다. "
                + "알려진 어긋남은 둘 다 닫혔습니다 — CY-718 은 선점문의 :stuckBefore "
                + "바인딩을 한 곳에 모았고, CY-743 은 배치 메타 시각을 호출부에서 "
                + "도메인 축으로 옮겼습니다(BatchTimeAxis). 둘 다 그 자리를 고친 것이지 "
                + "이 전제를 없앤 것이 아닙니다 — "
                + "앞으로 배치 메타 시각을 SQL 에 넣는 자리는 같은 함정을 다시 밟습니다. "
                + "컨테이너는 batch.yml 의 TZ, 로컬은 -Duser.timezone=UTC 로 맞추십시오.";
        if (required) {
            throw new IllegalStateException(message
                    + " 테스트처럼 일부러 다른 존을 쓴다면 환경변수 "
                    + "TIMEZONE_GUARD_REQUIRED=false (또는 실행 인자 --" + REQUIRED
                    + "=false) 로 거절을 끌 수 있습니다.");
        }
        log.error("JVM 기본 존 검사에 걸렸습니다 — 거절은 꺼져 있습니다({}=false). {}",
                REQUIRED, message);
    }

    /**
     * <b>끈 상태를 지표로 낸다.</b> 이 스택에는 Loki·promtail 이 없어 <b>로그가 감시 수단이
     * 아니다</b> — 형제 둘({@code cy_batch_schema_index_enforcement} ·
     * {@code cy_batch_jdbc_timeout_verified})이 같은 이유로 같은 모양을 쓴다. 이 축은
     * 증상이 아예 없어서(어긋난 started_at 도 판정을 안 바꾼다) 더 필요하다.
     */
    private static void publish(MeterRegistry registry, boolean required, ZoneId zone) {
        // 형제 둘은 여기서 registry null 을 보는데, 그 가지는 <b>도달할 수 없다</b> —
        // 생성자 주입에 필수 파라미터라 스프링이 null 을 못 넣고 넘기는 테스트도 없다.
        // 안 다루는 경우를 다루는 척하지 않는다. 형제 정리는 이 티켓 밖이다.
        Gauge.builder("cy_batch_timezone_enforcement", () -> required ? 1 : 0)
                .description("JVM 기본 존 거절이 켜져 있는가 — 1 켜짐 · 0 꺼짐")
                .register(registry);
        Gauge.builder("cy_batch_default_zone_verified", () -> isUtc(zone) ? 1 : 0)
                .description("JVM 기본 존이 고정 오프셋 0 인가 — 1 정상")
                .register(registry);
    }

    /**
     * <b>이름이 아니라 오프셋으로 본다.</b> {@code UTC}·{@code Etc/UTC}·{@code Z}·
     * {@code GMT} 가 모두 같은 좌표계인데 {@code ZoneId} 로는 서로 다른 객체다 — 이름을
     * 비교하면 멀쩡한 배포가 거절된다.
     *
     * <p><b>고정 오프셋만 받는다.</b> {@code Europe/London} 은 겨울에 오프셋이 0 이지만
     * 여름에 +1 이 된다 — 지금 0 이라고 통과시키면 <b>서머타임이 시작하는 날 조용히</b>
     * 어긋난다. 규칙에 전이가 있으면 거절한다.
     */
    static boolean isUtc(ZoneId zone) {
        // **벽시계를 안 읽는다.** 고정 오프셋만 통과시키므로 어느 순간을 물어도 답이 같다 —
        // Instant.EPOCH 로 묻는다. now() 를 쓰면 docs/04 의 금지에 걸리고
        // (NoWallClockInBatchTest 가 실제로 잡았다), 판정이 "언제 물었나" 에 매인다.
        return zone.getRules().isFixedOffset()
                && zone.getRules().getOffset(java.time.Instant.EPOCH).equals(ZoneOffset.UTC);
    }
}
