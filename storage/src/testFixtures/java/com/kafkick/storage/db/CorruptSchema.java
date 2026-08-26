// CORRUPT 스키마를 켜는 Flyway 로케이션을 한 곳에 둡니다.
package com.kafkick.storage.db;

import java.util.List;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

/**
 * <b>어노테이션에 쓸 수 있는 컴파일 타임 상수여야 합니다.</b> {@link CorruptRepositoryTest} 는
 * {@code @DataJpaTest} 기반이고 잡 테스트는 {@code @SpringBootTest} 라 애노테이션을 공유할 수 없는데,
 * 문자열까지 각자 들고 있으면 한쪽만 고쳐지는 날이 온다.
 *
 * <p>그때 배치 쪽이 CLEAN 에서 돌면 위반 INSERT 가 튕겨 시끄럽게 실패하지만,
 * <b>반대 방향은 조용하다</b> — "검출 0건" 단언은 CORRUPT 가 안 걸려도 초록이다.
 */
public final class CorruptSchema {

    /** 로케이션 목록에서 이 조각을 찾으면 CORRUPT 다. */
    private static final String CORRUPT_LOCATION = "db/corrupt";

    public static final String FLYWAY_LOCATIONS =
            "spring.flyway.locations=classpath:db/migration,classpath:db/corrupt";

    /**
     * <b>이 환경이 CORRUPT 스키마를 켰나.</b> 두 컨테이너 설정의 가드가 <b>같은 함수의 부정</b>을
     * 쓰게 하려고 여기 둔다 — 각자 판정하면 둘이 갈리는 날이 온다.
     *
     * <p><b>평문 문자열로 읽으면 안 된다.</b> {@code FlywayProperties.locations} 는
     * {@code List<String>} 이라 {@code spring.flyway.locations[0]=…} 인덱스 표기로도 바인딩된다.
     * 그때 인덱스 없는 키는 존재하지 않아 {@code getProperty} 가 {@code null} 을 주고,
     * <b>CORRUPT 가 켜져 있는데도 CLEAN 가드가 통과한다</b> — 공유 CLEAN 컨테이너에서
     * 제약이 떨어지고, 깨지는 것은 <i>나중에 도는 남의 테스트</i>다.
     *
     * <p>{@link Binder} 로 바인딩된 목록을 보면 표기법과 무관해진다.
     */
    public static boolean isCorrupt(Environment environment) {
        return Binder.get(environment)
                .bind("spring.flyway.locations", Bindable.listOf(String.class))
                .orElseGet(List::of)
                .stream()
                .anyMatch(location -> location.contains(CORRUPT_LOCATION));
    }

    /** 가드 메시지에 현재값을 싣는다. 무엇이 들어 있는지 안 보이면 고칠 수가 없다. */
    public static String locationsOf(Environment environment) {
        return String.join(",", Binder.get(environment)
                .bind("spring.flyway.locations", Bindable.listOf(String.class))
                .orElseGet(List::of));
    }

    private CorruptSchema() {
    }
}
