// 겹침 판정 규칙 자체를 못 박습니다. 파일 대조는 각 모듈 테스트가 합니다.
package com.kafkick.storage.db.config;

import static com.kafkick.storage.db.config.ConfigImportPrecedence.overlaps;
import static com.kafkick.storage.db.config.ConfigImportPrecedence.sameProperty;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>여기서 뚫리면 파일 대조가 전부 무의미해집니다.</b> 모듈 테스트는 "겹치는 키가 없다" 를
 * 단언하는데, 겹침 판정이 헐거우면 그 초록은 아무것도 뜻하지 않습니다.
 *
 * <p>Boot 의 바인딩은 문자열 일치가 아니라 <b>프로퍼티 트리</b>로 돕니다.
 */
class ConfigImportPrecedenceOverlapTest {

    @Test
    @DisplayName("표기법을 바꿔도 같은 프로퍼티로 본다 — 대시·카멜·인덱스")
    void namingStyleDoesNotHideACollision() {
        assertThat(overlaps("spring.datasource.hikari.maximumPoolSize",
                Set.of("spring.datasource.hikari.maximum-pool-size")))
                .as("relaxed binding 이 두 표기를 같은 프로퍼티로 묶는다")
                .isTrue();

        assertThat(overlaps("a[b]", Set.of("a.b")))
                .as("브래킷과 점 표기도 같은 프로퍼티다")
                .isTrue();
    }

    @Test
    @DisplayName("잎과 인덱스 항목은 같은 컬렉션이다 — 방향이 어느 쪽이든")
    void leafAndIndexedItemsAreTheSameCollection() {
        assertThat(overlaps("spring.flyway.locations[0]", Set.of("spring.flyway.locations")))
                .as("상위 소스가 잎을 주면 하위의 [0] 은 통째로 사라진다")
                .isTrue();
        assertThat(overlaps("spring.flyway.locations", Set.of("spring.flyway.locations[0]")))
                .as("반대 방향도 같은 컬렉션이다. 한쪽만 잡으면 표기에 따라 판정이 갈린다")
                .isTrue();
        assertThat(overlaps("spring.flyway.locations[1]", Set.of("spring.flyway.locations[0]")))
                .as("형제 인덱스도 같은 컬렉션이다 — Boot 는 항목을 준 첫 소스에서 멈춘다")
                .isTrue();
    }

    @Test
    @DisplayName("Map 으로 합쳐지는 루트는 조상 관계여도 겹침이 아니다")
    void mergedMapEntriesDoNotCollide() {
        assertThat(overlaps("logging.level.com.kafkick", Set.of("logging.level.com")))
                .as("MapBinder 는 소스별 엔트리를 합친다 — 둘 다 산다. "
                        + "겹침으로 잡으면 정당한 키를 실패시키고, 메시지가 `---` 아래로 내리라고 "
                        + "반대로 안내해 그 키를 검사 밖으로 밀어낸다")
                .isFalse();

        assertThat(overlaps("logging.level.com", Set.of("logging.level.com")))
                .as("정확히 같은 엔트리는 덮어쓰기가 맞다")
                .isTrue();
    }

    @Test
    @DisplayName("Map 루트 아래에서는 이름을 정규화하지 않는다 — 하이버네이트가 원문으로 받는다")
    void mergedMapRootsCompareRawNames() {
        assertThat(overlaps("spring.jpa.properties.hibernate.jdbc.timezone",
                Set.of("spring.jpa.properties.hibernate.jdbc.time_zone")))
                .as("하이버네이트에게 time_zone 과 timezone 은 다른 프로퍼티다. "
                        + "같다고 하면 죽은 오타 키를 `---` 아래로 밀어내라고 안내하게 된다")
                .isFalse();

        assertThat(overlaps("spring.jpa.properties.hibernate.jdbc.time_zone",
                Set.of("spring.jpa.properties.hibernate.jdbc.time_zone")))
                .as("원문이 같으면 덮어쓰기가 맞다")
                .isTrue();
    }

    @Test
    @DisplayName("sameProperty 는 조상 관계를 보지 않는다 — overlaps 와 질문이 다르다")
    void samePropertyIgnoresAncestors() {
        assertThat(overlaps("spring.datasource.hikari", Set.of("spring.datasource.hikari.maximum-pool-size")))
                .as("트리를 건드리는 것은 맞다 — 덮어쓰기 판정에서는 참이어야 한다")
                .isTrue();

        assertThat(sameProperty("spring.datasource.hikari", "spring.datasource.hikari.maximum-pool-size"))
                .as("hikari: 헤더만 남기고 자식을 주석 처리해도 그 자식이 살아 있다고 하면 안 된다")
                .isFalse();

        assertThat(sameProperty("spring.datasource.hikari.maximumPoolSize",
                "spring.datasource.hikari.maximum-pool-size"))
                .as("정규화 규칙은 overlaps 와 같아야 한다")
                .isTrue();
    }
}
