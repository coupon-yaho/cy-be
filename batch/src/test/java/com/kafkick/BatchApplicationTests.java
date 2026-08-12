package com.kafkick;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

@SpringBootTest
@Import(MySqlContainerConfig.class)
class BatchApplicationTests {

    @Test
    void contextLoads() {
    }

}
