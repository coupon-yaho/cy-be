// 두 바인딩 방식이 서버에 서로 다른 벽시계를 보낸다는 사실을 박아 둡니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>이 저장소의 시각 문서 전부가 이 한 가지 사실 위에 서 있다.</b> {@link DefaultZoneGuard} 도,
 * {@link com.kafkick.batch.api.StuckRunClaim} 도, {@code CleanupJobConfig} 의 메타 컷오프도
 * <i>"어느 쪽 바인딩이 존 정규화를 타는가"</i> 로 옳고 그름이 갈린다. 그래서 주석으로 두지 않고
 * <b>서버가 실제로 본 값</b>을 단언한다.
 *
 * <p><b>그래서 방향이 자리마다 반대다.</b> 자바 쪽 값이 <b>JVM 기본 존</b>이면
 * ({@code LocalDateTime.now()} · 배치 메타를 읽어 온 값) {@code Timestamp.valueOf} 로 감싸야
 * UTC 컬럼과 만나고, 자바 쪽 값이 이미 <b>UTC</b>면({@code TimeProvider}) <b>감싸면 안 된다</b> —
 * 감싸는 순간 존 오프셋만큼 밀린다. 한때 이 파일이 없어서 후자를 <i>"여기도 깨진다"</i> 로
 * 잘못 적었다.
 */
@SpringBootTest(properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000",
        "server.port=0",
        "management.server.port=0"
})
@Import(MySqlContainerConfig.class)
class TimestampBindingAxisTest {

    private static final LocalDateTime WALL = LocalDateTime.of(2026, 7, 1, 16, 42, 55);
    private static final String FORMAT = "SELECT DATE_FORMAT(:t,'%Y-%m-%d %H:%i:%s')";

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    void rawLocalDateTimeReachesTheServerUnchanged() {
        assertThat(sentToServer(WALL))
                .as("원시 LocalDateTime 은 드라이버의 존 변환을 안 탄다 — 자바 쪽 값이 "
                        + "이미 UTC 인 자리(TimeProvider)는 이대로가 맞다")
                .isEqualTo("2026-07-01 16:42:55");
    }

    @Test
    void timestampValueOfIsRenderedInTheSessionZone() {
        ZoneId jvm = ZoneId.systemDefault();
        assumeThat(jvm.getRules().getOffset(WALL.toInstant(ZoneOffset.UTC)))
                .as("JVM 이 UTC 면 두 방식이 같은 값을 보내 이 축을 못 잰다")
                .isNotEqualTo(ZoneOffset.UTC);

        // 세션 존은 UTC 로 못 박혀 있다(접속 URL). 그래서 기대값이 오프셋만큼 뒤로 간다.
        assertThat(jdbcClient.sql("SELECT @@session.time_zone").query(String.class).single())
                .isEqualTo("+00:00");
        String expected = WALL.atZone(jvm).withZoneSameInstant(ZoneOffset.UTC)
                .toLocalDateTime().toString().replace('T', ' ');

        assertThat(sentToServer(Timestamp.valueOf(WALL)))
                .as("Timestamp.valueOf 는 JVM 기본 존 → 세션 존으로 정규화된다 — "
                        + "스프링 배치가 메타 시각을 이 경로로 쓰므로, 자바 쪽 값이 "
                        + "JVM 기본 존인 자리는 이렇게 감싸야 컬럼과 만난다")
                .isEqualTo(expected);
    }

    private String sentToServer(Object bound) {
        return jdbcClient.sql(FORMAT).param("t", bound).query(String.class).single();
    }
}
