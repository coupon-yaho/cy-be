// JVM 기본 타임존이 UTC 인지 기동 시점에 확인합니다.
package com.kafkick.batch.config;

import java.time.ZoneId;
import java.time.ZoneOffset;

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
 * <p><b>어긋나면 시체 판정이 통째로 뒤집힌다.</b> {@link RunningJobProbe} 는 그 메타 시각을
 * {@code LocalDateTime.now()} 와 빼서 진도를 재는데, MySQL 세션 존은 {@code base.yml} 이
 * UTC 로 못 박았다. KST JVM 이면 <b>모든 실행이 아홉 시간 늙은 것으로 보여</b> 죄다 시체가
 * 되고, {@code blockingExecutions} 가 빈 목록을 돌려주면서 <b>만료·검증의 상호 배제가
 * 조용히 꺼진다.</b>
 *
 * <p><b>지금 UTC 를 세 군데서 따로 박고 있다</b> — {@code batch.yml} 의 {@code TZ},
 * {@code batch/build.gradle} 의 {@code bootRun} {@code user.timezone}, 그리고 컨테이너 이미지.
 * 어느 하나가 빠져도 기동은 성공하고 증상은 <b>가드가 안 우는 것</b>으로만 나타난다 —
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

    public DefaultZoneGuard(@Value("${" + REQUIRED + ":true}") boolean required) {
        ZoneId zone = ZoneId.systemDefault();
        if (isUtc(zone)) {
            log.info("JVM 기본 존 확인 완료 — {} 로 배치 메타와 같은 좌표계입니다.", zone);
            return;
        }
        String message = "JVM 기본 존이 UTC 가 아닙니다: " + zone + ". "
                + "스프링 배치가 BATCH_JOB_EXECUTION.START_TIME 과 "
                + "BATCH_STEP_EXECUTION.LAST_UPDATED 를 이 존으로 찍는데 MySQL 세션 존은 "
                + "UTC 라, RunningJobProbe 의 시체 판정이 그 차이만큼 어긋나고 "
                + "만료·검증의 상호 배제가 조용히 꺼집니다. "
                + "컨테이너는 batch.yml 의 TZ, 로컬은 -Duser.timezone=UTC 로 맞추십시오. "
                + "테스트처럼 일부러 다른 존을 쓴다면 환경변수 "
                + "TIMEZONE_GUARD_REQUIRED=false (또는 실행 인자 --" + REQUIRED + "=false) "
                + "로 거절을 끌 수 있습니다.";
        if (required) {
            throw new IllegalStateException(message);
        }
        log.error("JVM 기본 존 검사에 걸렸습니다 — 거절은 꺼져 있습니다({}=false). {}",
                REQUIRED, message);
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
