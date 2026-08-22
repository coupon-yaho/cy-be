// CLEAN 스키마가 시드 로더의 DDL 과 같은 최종 상태를 만드는지 확인합니다.
package com.kafkick.storage.db;

import java.util.List;

/** 게이트의 정상셋이 도는 모양이다. 제약이 전부 걸린 쪽. */
@RepositoryTest
class CleanSchemaParityTest extends SchemaParityTestBase {

    @Override
    List<String> seedDdl() {
        return CLEAN_DDL;
    }

    @Override
    String seedSchema() {
        return "seed_parity_clean";
    }
}
