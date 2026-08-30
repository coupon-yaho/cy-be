// 정리 어댑터가 트랜잭션 계약을 애너테이션으로 지고 있는지 확인합니다.
package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * <b>애너테이션 한 줄이 계약 전부인 자리는 그 줄을 재야 한다.</b> 지금 이 메서드를 부르는 곳은
 * 태스클릿 하나뿐이고 그쪽은 언제나 트랜잭션 안이라, {@code @Transactional} 을 <b>지워도 전
 * 테스트가 초록이다</b>(실제로 확인했다). 그러면 계약이 조용히 사라진다.
 *
 * <p><b>왜 {@code MANDATORY} 인가.</b> 이 메서드는 여섯 문장으로 실행·자식·고아 인스턴스를
 * 지우고, 그 원자성이 계약이다. 나중에 관리 API 나 다른 스케줄러가 트랜잭션 없이 부르면
 * 문장마다 자동 커밋되고, 실행을 지운 뒤 죽으면 <b>고아 인스턴스만 남은 중간 상태</b>가
 * 남는다. {@code REQUIRED} 면 그 호출이 조용히 성공하고, {@code MANDATORY} 면 <b>첫
 * 호출에서</b> 거절된다 — 태스클릿 경로는 그대로 조인한다.
 *
 * <p><b>컨테이너를 안 띄운다.</b> 재는 것이 리플렉션으로 읽는 애너테이션이라 DB 가 필요 없다.
 */
class CleanupJdbcAdapterContractTest {

    @Test
    @DisplayName("배치 메타 삭제는 MANDATORY 다 — 트랜잭션 없는 호출을 첫 호출에서 거절한다")
    void deleteBatchMetadataChunkIsMandatory() throws Exception {
        Transactional annotation = CleanupJdbcAdapter.class
                .getMethod("deleteBatchMetadataChunk", LocalDateTime.class, int.class)
                .getAnnotation(Transactional.class);

        assertThat(annotation)
                .as("여섯 문장의 원자성이 이 메서드의 계약이다 — 애너테이션이 그것을 진다")
                .isNotNull();
        assertThat(annotation.propagation())
                .as("REQUIRED 로 내리면 트랜잭션 없는 호출이 문장마다 자동 커밋돼 "
                        + "고아 인스턴스만 남은 중간 상태를 만든다")
                .isEqualTo(Propagation.MANDATORY);
    }
}
