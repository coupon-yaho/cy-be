package com.kafkick;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.stream.Collectors;

import jakarta.persistence.Entity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

/**
 * batch 가 JPA 를 뺀 채로 도는 것은 "엔티티가 0개" 라는 사실에 기대고 있다. 그 전제가 깨지는
 * 순간을 알려 준다 — 주석으로 남긴 TODO 는 아무도 읽지 않고, 이 전제는 조용히 깨지면
 * 기동 실패나 감사 필드 누락으로 나타난다.
 *
 * <p>지금 저장소에는 {@code @Entity} 가 하나도 없다. {@code BaseEntity}·{@code UpdatableEntity} 는
 * {@code @MappedSuperclass} 라 JPA 메타모델의 관리 타입으로 세어지지 않는다. 그래서
 * {@code @EnableJpaAuditing} 이 만드는 jpaMappingContext 가 "JPA metamodel must not be empty" 로
 * 기동을 세운다.
 *
 * <p>이 테스트가 깨지면 엔티티가 생겼다는 뜻이다. 그때 판단할 것 —
 * <ul>
 *   <li>배치가 여전히 JDBC 로만 돈다면 batch 의 스위치를 그대로 두고 이 테스트의 기대만 바꾼다.
 *   <li>배치가 JPA 를 쓰기 시작한다면 {@code spring.autoconfigure.exclude} 의 JPA 두 줄과
 *       {@code storage.jpa.auditing.enabled: false} 를 함께 걷는다. 하나만 걷으면 기동에서 죽는다.
 * </ul>
 */
class BatchJpaFreeAssumptionTest {

    @Test
    @DisplayName("엔티티가 0개라는 전제 위에 batch 의 JPA 제외가 서 있다")
    void noJpaEntitiesExistYet() {
        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(Entity.class));

        Set<String> entities = scanner.findCandidateComponents("com.kafkick").stream()
                .map(BeanDefinition::getBeanClassName)
                .collect(Collectors.toSet());

        assertThat(entities)
                .as("엔티티가 생겼다. batch 의 JPA 자동설정 제외와 storage.jpa.auditing.enabled 를"
                        + " 다시 판단한다 — 클래스 주석의 두 갈래를 보라")
                .isEmpty();
    }
}
