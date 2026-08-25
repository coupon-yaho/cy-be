// 후보에서 첫 회차의 연속부를 자르는 규칙을 확인합니다.
package com.kafkick.core.expiration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * <b>이 자르기가 락 순서를 성립시킨다.</b> 잠글 재고 행을 <b>쓰기 전에</b> 하나로 정하는 것이
 * {@link ExpireChunk#from} 이고, 그것이 되어야 만료가 {@code coupon_stocks} 를 먼저 잡을 수
 * 있다({@link ExpirationRepository} 의 1213 재현 참고).
 *
 * <p><b>DB 가 필요 없다.</b> 규칙이 순수 함수라 여기서 전부 재고, 통합 테스트는 이 규칙이
 * 실제 SQL 결과 위에서도 성립하는지만 본다.
 */
class ExpireChunkTest {

    private static List<ExpireCandidate> candidates(long... idAndCoupon) {
        List<ExpireCandidate> out = new java.util.ArrayList<>();
        for (int i = 0; i < idAndCoupon.length; i += 2) {
            out.add(new ExpireCandidate(idAndCoupon[i], idAndCoupon[i + 1]));
        }
        return List.copyOf(out);
    }

    @Nested
    @DisplayName("연속부 자르기")
    class Prefix {

        @Test
        @DisplayName("후보가 전부 같은 회차면 통째로 한 청크다")
        void takesAllWhenSingleCoupon() {
            ExpireChunk chunk = ExpireChunk.from(candidates(10, 7, 11, 7, 12, 7));

            assertThat(chunk.couponId()).isEqualTo(7);
            assertThat(chunk.lastId())
                    .as("마지막 후보의 id 가 상한이 된다 — afterId 가 여기까지 밀린다")
                    .isEqualTo(12);
            assertThat(chunk.size()).isEqualTo(3);
        }

        @Test
        @DisplayName("회차가 바뀌는 자리에서 끊는다 — 뒤는 다음 청크 몫이다")
        void stopsAtCouponBoundary() {
            ExpireChunk chunk = ExpireChunk.from(candidates(10, 7, 11, 7, 12, 8, 13, 8));

            assertThat(chunk.couponId()).isEqualTo(7);
            assertThat(chunk.lastId())
                    .as("12 를 상한으로 잡으면 회차 8 의 12번이 만료 없이 afterId 아래로 묻힌다")
                    .isEqualTo(11);
            assertThat(chunk.size()).isEqualTo(2);
        }

        @Test
        @DisplayName("첫 회차가 한 건뿐이어도 그 한 건으로 청크를 만든다")
        void singleRowChunkIsValid() {
            ExpireChunk chunk = ExpireChunk.from(candidates(10, 7, 11, 8, 12, 8));

            assertThat(chunk.couponId()).isEqualTo(7);
            assertThat(chunk.lastId()).isEqualTo(10);
            assertThat(chunk.size())
                    .as("느릴 뿐 틀리지 않다. 이런 청크가 이어지는지는 chunkFill 이 본다")
                    .isEqualTo(1);
        }

        @Test
        @DisplayName("같은 회차가 뒤에 또 나와도 끊긴 뒤는 안 줍는다")
        void doesNotReachPastTheBoundary() {
            ExpireChunk chunk = ExpireChunk.from(candidates(10, 7, 11, 8, 12, 7));

            assertThat(chunk.lastId())
                    .as("id 구간으로 진도를 내므로 건너뛴 것을 주우면 12 아래가 통째로 묻힌다")
                    .isEqualTo(10);
            assertThat(chunk.size()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("끝내는 신호")
    class Termination {

        @Test
        @DisplayName("후보가 없으면 빈 청크다 — 이것이 잡의 종료 조건이다")
        void emptyCandidatesTerminate() {
            assertThat(ExpireChunk.from(List.of()).isEmpty()).isTrue();
            assertThat(ExpireChunk.from(null).isEmpty()).isTrue();
            assertThat(ExpireChunk.EMPTY.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("후보가 하나라도 있으면 비어 있지 않다 — 넘어갈지는 UPDATE 가 정한다")
        void nonEmptyEvenIfNothingWillExpire() {
            assertThat(ExpireChunk.from(candidates(10, 7)).isEmpty())
                    .as("이 사이에 전부 사용됐어도 진도는 나가야 한다. "
                            + "예전처럼 만료 0 을 종료로 읽으면 같은 자리를 맴돈다")
                    .isFalse();
        }
    }

    @Test
    @DisplayName("후보가 id 오름차순이 아니면 던진다 — 조용히 정렬하면 누락을 덮는다")
    void rejectsUnorderedCandidates() {
        assertThatThrownBy(() -> ExpireChunk.from(candidates(11, 7, 10, 7)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("오름차순")
                .hasMessageContaining("11")
                .hasMessageContaining("10");
    }

    @Test
    @DisplayName("같은 id 가 두 번 오는 것도 오름차순 위반이다")
    void rejectsDuplicateIds() {
        assertThatThrownBy(() -> ExpireChunk.from(candidates(10, 7, 10, 7)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("오름차순");
    }

    @Test
    @DisplayName("크기가 음수인 청크는 만들 수 없다")
    void rejectsNegativeSize() {
        assertThatThrownBy(() -> new ExpireChunk(7L, 10L, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
