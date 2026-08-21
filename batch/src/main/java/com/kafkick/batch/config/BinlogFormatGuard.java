// 만료 Step 의 격리 인하가 성립하는 서버인지 기동 때 확인합니다.
package com.kafkick.batch.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * <b>만료 Step 은 READ COMMITTED 로 돈다. 그 전제가 서버 설정에 걸려 있다.</b>
 *
 * <p>MySQL 은 {@code log_bin=ON} 이면서 {@code binlog_format=STATEMENT} 인 서버에서
 * READ COMMITTED 트랜잭션의 InnoDB DML 을 <b>오류 1665 로 거부한다.</b> 만료 배치의 첫
 * 문장이 그 DML 이므로, 그런 서버에 배포하면 <b>5분 뒤 첫 주기부터 전부 실패한다.</b>
 *
 * <p><b>전제를 알면서 확인하지 않는 것이 문제였다.</b> 코드 주석과 측정 문서가 이 조건을
 * 적어 두었지만, 확인하는 코드가 없어 발견 시점이 "배포 후 첫 주기" 였다. 그때 화면에 뜨는
 * 것은 1665 라는 숫자뿐이고, 거기서 "만료 Step 의 격리 인하 때문" 까지 가는 경로가 어디에도
 * 없었다.
 *
 * <p><b>테스트 컨테이너로는 재현할 수 없다.</b> {@code MySqlContainerConfig} 가
 * {@code --skip-log-bin} 으로 띄우기 때문이다(트리거 생성이 오류 1419 로 막히는 것을 푸는
 * 설정이다). 그래서 이 가드는 CI 에서 <b>통과 경로만</b> 밟는다 — 아래
 * {@code log_bin} 검사가 그 경로다.
 *
 * <p>기동을 막는 쪽으로 정했다. 이 서버에서는 만료가 <b>한 번도 성공할 수 없고</b>,
 * 재고를 되돌리는 유일한 배치가 조용히 안 도는 것보다 안 뜨는 것이 낫다.
 */
@Component
public class BinlogFormatGuard {

    private static final Logger log = LoggerFactory.getLogger(BinlogFormatGuard.class);

    private final JdbcClient jdbcClient;

    public BinlogFormatGuard(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void assertReadCommittedIsUsable() {
        // binlog 가 꺼져 있으면 format 값과 무관하게 제약이 없다. 테스트 컨테이너가 그 경로다.
        boolean binlogOn = jdbcClient.sql("SELECT @@GLOBAL.log_bin")
                .query(Boolean.class)
                .single();
        String format = jdbcClient.sql("SELECT @@GLOBAL.binlog_format")
                .query(String.class)
                .single();

        if (binlogOn && "STATEMENT".equalsIgnoreCase(format)) {
            throw new IllegalStateException(
                    "binlog_format=STATEMENT 에서는 만료 Step 의 READ COMMITTED DML 이 "
                            + "오류 1665 로 거부됩니다. 만료가 매 주기 실패하므로 기동을 막습니다. "
                            + "ROW 또는 MIXED 로 바꾸거나, 격리 인하를 되돌리고 그 대가"
                            + "(만료가 도는 동안 발급 INSERT 가 1205 로 막힘)를 받아야 합니다. "
                            + "근거는 docs/12-expire-lock-measurement.md 에 있습니다.");
        }
        log.info("만료 Step 의 READ COMMITTED 전제를 확인했습니다. log_bin={} binlog_format={}",
                binlogOn, format);
    }
}
