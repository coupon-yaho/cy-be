// STATEMENT binlog 서버에서 기동이 막히는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * <b>이 조합만은 공용 컨테이너로 못 잰다.</b> {@code MySqlContainerConfig} 는
 * {@code --skip-log-bin} 으로 띄우기 때문에(트리거 생성이 오류 1419 로 막히는 것을 푸는
 * 설정이다) STATEMENT binlog 를 절대 밟지 않는다. 그래서 여기서만 컨테이너를 따로 띄운다.
 *
 * <p><b>왜 굳이 따로 띄우나.</b> 이 가드가 막는 사고는 <i>"레거시 my.cnf 를 가져온 서버에
 * 배포하면 5분 뒤 첫 만료 주기부터 전부 실패한다"</i> 이고, 그 실패는 오류 1665 라는 숫자로만
 * 나온다. 가드가 정말 도는지 확인하지 않으면 <b>주석으로만 존재하는 전제</b>가 하나 더 생긴다 —
 * 이 저장소가 반복해서 겪은 결함이다.
 *
 * <p>두 방향을 다 본다. STATEMENT 면 막고, ROW 면 통과해야 한다. 막는 쪽만 보면
 * <b>항상 던지는 가드</b>도 통과한다.
 *
 * <p><b>배선은 여기서 안 잰다.</b> 이 클래스는 객체를 직접 만들어 메서드를 부르므로
 * {@code @Component} 나 {@code expireJob} 의 {@code listener(...)} 등록을 지워도 통과한다.
 * 그 축은 {@code BinlogFormatGuardWiringTest} 가 실제 컨텍스트에서 잰다 —
 * 가드가 <b>잡에 붙어 있는지</b>가 이 가드의 값어치 전부이기 때문이다.
 */
class BinlogFormatGuardTest {

    private static MySQLContainer statementBinlog;
    private static MySQLContainer rowBinlog;

    @BeforeAll
    static void startContainers() {
        statementBinlog = mysql("STATEMENT");
        rowBinlog = mysql("ROW");
        statementBinlog.start();
        rowBinlog.start();
    }

    @AfterAll
    static void stopContainers() {
        statementBinlog.stop();
        rowBinlog.stop();
    }

    @Test
    @DisplayName("binlog_format=STATEMENT 면 기동이 막힌다")
    void refusesToStartOnStatementBinlog() {
        assertThatThrownBy(() -> guardFor(statementBinlog).assertReadCommittedIsUsable())
                .as("여기서 안 막으면 배포 5분 뒤 첫 청크가 오류 1665 로 죽고, "
                        + "그 숫자에서 '만료 Step 의 격리 인하' 까지 가는 경로가 없다")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("binlog_format=STATEMENT")
                .hasMessageContaining("1665");
    }

    @Test
    @DisplayName("binlog_format=ROW 면 통과한다")
    void allowsRowBinlog() {
        assertThatCode(() -> guardFor(rowBinlog).assertReadCommittedIsUsable())
                .as("항상 던지는 가드는 가드가 아니다")
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("두 컨테이너가 실제로 다른 설정으로 떴는지 먼저 확인한다")
    void containersActuallyDiffer() {
        assertThat(formatOf(statementBinlog))
                .as("이것이 틀리면 위 두 단언이 같은 조건을 두 번 재는 셈이다")
                .isEqualToIgnoringCase("STATEMENT");
        assertThat(formatOf(rowBinlog)).isEqualToIgnoringCase("ROW");
    }

    private static MySQLContainer mysql(String binlogFormat) {
        return new MySQLContainer(DockerImageName.parse("mysql:latest"))
                .withDatabaseName("t")
                .withUsername("test")
                .withCommand("--log-bin=binlog",
                        "--binlog-format=" + binlogFormat,
                        "--server-id=1");
    }

    private static BinlogFormatGuard guardFor(MySQLContainer container) {
        return new BinlogFormatGuard(JdbcClient.create(dataSourceFor(container)));
    }

    private static String formatOf(MySQLContainer container) {
        return JdbcClient.create(dataSourceFor(container))
                .sql("SELECT @@GLOBAL.binlog_format")
                .query(String.class)
                .single();
    }

    private static DataSource dataSourceFor(MySQLContainer container) {
        return DataSourceBuilder.create()
                .url(container.getJdbcUrl())
                .username(container.getUsername())
                .password(container.getPassword())
                .build();
    }
}
