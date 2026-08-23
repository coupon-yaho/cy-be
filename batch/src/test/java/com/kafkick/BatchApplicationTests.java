package com.kafkick;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;

import com.kafkick.core.admin.overview.AdminOverviewService;
import com.kafkick.storage.db.MySqlContainerConfig;

@SpringBootTest
@Import(MySqlContainerConfig.class)
class BatchApplicationTests {

    @Autowired
    private ApplicationContext context;

    /** API 전용 Overview 원천이 없는 Batch 전체 Context는 Overview Service 없이 기동합니다. */
    @Test
    void contextLoads() {
        assertThat(context.getBeansOfType(AdminOverviewService.class)).isEmpty();
    }

}
