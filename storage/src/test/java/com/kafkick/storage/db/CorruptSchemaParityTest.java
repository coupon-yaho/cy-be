// CORRUPT 스키마가 시드 로더의 DDL 과 같은 최종 상태를 만드는지 확인합니다.
package com.kafkick.storage.db;

import java.util.List;

/**
 * <b>오염셋 800행 판정이 실제로 도는 쪽이다.</b> CLEAN 만 대조하면 정작 검증이 도는 모양은
 * 안 본 채로 남는다.
 *
 * <p><b>만드는 방향이 반대다.</b> 시드는 CLEAN 전용 제약을 <b>안 걸고</b>, cy-be 는 전부 건 뒤
 * {@code V900__drop_clean_only_constraints.sql} 로 <b>떼어 낸다.</b> 같은 최종 상태에 도달하는지가
 * 이 대조의 질문이고, 걷어내는 쪽은 자동 생성된 제약 이름까지 알아야 해서 갈리기 쉽다.
 */
@CorruptRepositoryTest
class CorruptSchemaParityTest extends SchemaParityTestBase {

    @Override
    List<String> seedDdl() {
        return CORRUPT_DDL;
    }

    @Override
    String seedSchema() {
        return "seed_parity_corrupt";
    }
}
