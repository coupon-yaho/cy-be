package com.kafkick;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;

import com.kafkick.core.admin.overview.AdminOverviewService;
import com.kafkick.storage.db.MySqlContainerConfig;

// JPA 스키마 검증이 켜져 있다(storage.yml 의 ddl-auto: validate). batch 는 마이그레이션
// 소유자가 아니라 spring.flyway.enabled 가 false 인데, 테스트 컨테이너는 빈 DB 로 뜨므로
// 여기서만 켜서 스키마를 만든다 — 배치 메타 테스트들이 이미 쓰는 패턴이다.
@SpringBootTest(properties = "spring.flyway.enabled=true")
@Import(MySqlContainerConfig.class)
class BatchApplicationTests {

    @Autowired
    private ApplicationContext context;

    /** API 전용 Overview 원천이 없는 Batch 전체 Context는 Overview Service 없이 기동합니다. */
    @Test
    void startsBatchContextWithoutAdminOverviewService() {
        assertThat(context.getBeansOfType(AdminOverviewService.class)).isEmpty();
    }

}
