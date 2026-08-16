// CORRUPT 스키마 모양(제약 셋이 없는) 위에서 도는 리포지터리 테스트입니다.
package com.kafkick.storage.db;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

/**
 * <b>{@link RepositoryTest} 와 다른 점은 Flyway 로케이션 하나뿐입니다.</b>
 * {@code db/corrupt} 를 뒤에 붙여 CLEAN 전용 제약 셋을 떨어뜨립니다 —
 * {@code uk_coupon_member} · {@code issuances.code} UNIQUE · {@code ck_stock_range}.
 *
 * <p><b>왜 제약을 떼야 하는가.</b> V2 는 같은 회원이 같은 회차에서 두 번 발급된 것을 찾는데,
 * 그 행이 {@code uk_coupon_member} 때문에 애초에 INSERT 되지 않습니다. 제약을 남긴 채
 * 테스트를 쓰면 <b>규칙이 틀려도 "검출 0건" 이라 초록</b>입니다.
 *
 * <p><b>왜 테스트 안에서 인덱스를 떨어뜨리지 않는가.</b> MySQL 의 DDL 은 암묵 커밋이라
 * {@code @DataJpaTest} 의 롤백 격리가 그 자리에서 깨집니다. 앞 테스트가 남긴 행이
 * 뒤 테스트로 새고, 실패가 실행 순서에 따라 달라집니다.
 *
 * <p><b>컨텍스트가 하나 더 뜹니다.</b> 프로퍼티가 다르면 스프링이 컨텍스트를 따로 캐시하고,
 * {@link MySqlContainerConfig} 가 빈이라 MySQL 컨테이너도 한 벌 더 뜹니다. 느린 값이므로
 * <b>제약이 없어야만 되는 테스트에만</b> 씁니다 — 나머지는 {@link RepositoryTest} 입니다.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(MySqlContainerConfig.class)
@TestPropertySource(properties = CorruptSchema.FLYWAY_LOCATIONS)
public @interface CorruptRepositoryTest {
}
