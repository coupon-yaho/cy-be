package com.kafkick;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.core.benchmark.RunTimeseriesArchiver;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.ArchiveStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.test.util.AopTestUtils;
import com.kafkick.storage.db.benchmark.JdbcRunTimeseriesArchiveStore;
import com.kafkick.api.admin.observability.PromQueryClient;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "observation.datasource.enabled=true")
@Import(MySqlContainerConfig.class)
class ApiApplicationTests {

    @Test
    void contextLoads(@Autowired RunTimeseriesArchiver archiver, @Autowired ArchiveStore store,
                      @Autowired JdbcTemplate main, @Qualifier("obs") JdbcTemplate observation,
                      @Qualifier("promQueryClient") PromQueryClient pollingClient,
                      @Qualifier("archivePromQueryClient") PromQueryClient archiveClient) {
        assertThat(archiver).isNotNull();
        assertThat(store).isNotNull();
        assertThat(main).isNotSameAs(observation);
        assertThat(pollingClient).isNotSameAs(archiveClient);
        assertThat(store).isInstanceOf(JdbcRunTimeseriesArchiveStore.class);
        Object target = AopTestUtils.getUltimateTargetObject(store);
        assertThat(new DirectFieldAccessor(target).getPropertyValue("writeJdbcTemplate"))
                .isSameAs(main).isNotSameAs(observation);
    }

}
