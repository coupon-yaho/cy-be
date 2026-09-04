// SKIP LOCKED 선점의 복제 안전 전제를 기동 시 확인합니다.
package com.kafkick.infra.mq.notification;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;


/**
 * <b>{@code SKIP LOCKED} 는 statement-based replication 에 unsafe 다.</b>
 * MySQL 레퍼런스가 명시한다 — 잠긴 행을 건너뛰므로 <b>같은 문장이 서버마다 다른 결과</b>를
 * 낸다. 복제본이 원본과 갈린다.
 *
 * <h2>왜 {@code BinlogFormatGuard} 로 안 되나</h2>
 *
 * <p>이 저장소에 같은 검사가 이미 있다. 하지만 그것은 <b>{@code batch} 모듈</b>의
 * {@code expireJob} 리스너이고, <b>알림 릴레이는 {@code api} 애플리케이션에서 돈다</b>
 * ({@code kafka.yml} 이 {@code api/src/main/resources} 에 있다).
 *
 * <p>그리고 {@code api} 는 <b>{@code batch} 를 의존하지 않는다</b> — {@code :core} ·
 * {@code :storage} · {@code :infra:redis} · {@code :infra:mq} 는 물지만 {@code :batch} 는
 * 없다. 그래서 그 가드를 <b>클래스패스에서 볼 수조차 없다.</b>
 *
 * <p>한때 PR 본문에 <i>"기존 가드가 이 전제를 지킨다"</i> 고 적었는데 사실이 아니었다
 * (Qodo 리뷰가 잡았다). 전제를 쓰는 쪽에 검사를 둔다.
 *
 * <h2>기동을 막지 않는다</h2>
 *
 * <p>WARN 만 남긴다. 복제 정합성은 <b>운영 환경의 문제</b>이지 이 프로세스가 일을 못 한다는
 * 뜻이 아니고, 알림 릴레이가 기동을 막으면 접수 API 전체가 안 뜬다 —
 * <i>"기동 거부가 아니라 눈에 보이게"</i> 가 이 저장소가 가드에 대해 세운 방향이다.
 *
 * <p>{@code binlog} 자체가 꺼져 있으면 포맷과 무관하게 제약이 없다. 테스트 컨테이너가 그 경로다.
 */
@Order(0)
public class RelayBinlogFormatGuard implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RelayBinlogFormatGuard.class);

    private final DataSource dataSource;

    /**
     * <b>{@code JdbcClient} 를 안 쓴다.</b> {@code infra:mq} 는 spring-jdbc 를 의존하지 않고,
     * 이 검사 하나 때문에 모듈 의존을 늘리는 것은 값이 안 맞는다. 표준 JDBC 로 두 값만 읽는다.
     */
    public RelayBinlogFormatGuard(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void run(ApplicationArguments args) {
        boolean binlogOn;
        String format;
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(
                        "SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format")) {
            if (!rs.next()) {
                log.warn("binlog 설정을 못 읽어 SKIP LOCKED 의 복제 안전 전제를 "
                        + "확인하지 못했습니다. 결과가 비었습니다.");
                return;
            }
            binlogOn = rs.getBoolean(1);
            format = rs.getString(2);
        } catch (Exception unreadable) {
            // 권한이 없거나 MySQL 이 아닐 수 있다. 확인 못 한 것을 확인한 것처럼 넘어가지 않는다.
            log.warn("binlog 설정을 못 읽어 SKIP LOCKED 의 복제 안전 전제를 확인하지 못했습니다. "
                    + "사유={}", unreadable.toString());
            return;
        }

        if (binlogOn && "STATEMENT".equalsIgnoreCase(format)) {
            log.warn("binlog_format=STATEMENT 입니다. outbox 선점의 SKIP LOCKED 는 잠긴 행을 "
                    + "건너뛰므로 같은 문장이 서버마다 다른 결과를 냅니다 — 복제본이 원본과 "
                    + "갈립니다. ROW 로 바꾸십시오.");
            return;
        }

        log.info("outbox 선점의 복제 안전 전제를 확인했습니다. log_bin={} binlog_format={}",
                binlogOn, format);
    }
}
