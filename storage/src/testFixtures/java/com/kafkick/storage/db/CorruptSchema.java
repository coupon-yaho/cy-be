// CORRUPT 스키마를 켜는 Flyway 로케이션을 한 곳에 둡니다.
package com.kafkick.storage.db;

/**
 * <b>어노테이션에 쓸 수 있는 컴파일 타임 상수여야 합니다.</b> {@link CorruptRepositoryTest} 는
 * {@code @DataJpaTest} 기반이고 잡 테스트는 {@code @SpringBootTest} 라 애노테이션을 공유할 수 없는데,
 * 문자열까지 각자 들고 있으면 한쪽만 고쳐지는 날이 온다.
 *
 * <p>그때 배치 쪽이 CLEAN 에서 돌면 위반 INSERT 가 튕겨 시끄럽게 실패하지만,
 * <b>반대 방향은 조용하다</b> — "검출 0건" 단언은 CORRUPT 가 안 걸려도 초록이다.
 */
public final class CorruptSchema {

    public static final String FLYWAY_LOCATIONS =
            "spring.flyway.locations=classpath:db/migration,classpath:db/corrupt";

    private CorruptSchema() {
    }
}
