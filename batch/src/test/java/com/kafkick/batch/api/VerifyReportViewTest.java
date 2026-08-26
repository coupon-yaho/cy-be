// 리포트가 "0건" 과 "안 돌렸다" 를 갈라 보여주는지 확인합니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingKey;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;

/**
 * <b>DB 가 필요 없다.</b> 이 뷰가 지는 계약이 순수 함수라 여기서 전수로 잰다 —
 * 실제 SQL 위에서의 계약은 {@code VerificationFindingJdbcAdapterTest} 가 본다.
 *
 * <p><b>제출물이라 모양이 곧 계약이다.</b> 필드가 빠지거나 0이 사라지면 읽는 사람이
 * 판정을 잘못 읽는데, 그 순간에는 아무것도 안 깨진다.
 */
class VerifyReportViewTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    private static VerificationRun run(DatasetType dataset, Long seedRunId) {
        return VerificationRun.restore(7L, AS_OF, null, ScopeType.FULL, dataset, 1,
                VerdictType.PASS, StatsStatus.COMPLETE, 0,
                "checksum", "fingerprint", AS_OF.plusMinutes(1), AS_OF.plusMinutes(9),
                seedRunId);
    }

    @Nested
    @DisplayName("규칙별 검출")
    class ByType {

        @Test
        @DisplayName("검출이 0인 규칙도 0으로 채워서 준다 — 빠지면 '안 돌렸다' 로 읽힌다")
        void fillsZeroForRulesWithoutFindings() {
            VerifyReportView view = VerifyReportView.of(
                    run(DatasetType.CLEAN, null),
                    Map.of(FindingType.STOCK_MISMATCH, 3),
                    null);

            assertThat(view.byType())
                    .as("여섯 규칙이 전부 있어야 한다. 저장소는 GROUP BY 라 있는 것만 준다")
                    .hasSize(FindingType.values().length)
                    .containsEntry(FindingType.STOCK_MISMATCH, 3)
                    .containsEntry(FindingType.REPLAY_MISMATCH, 0);
        }

        @Test
        @DisplayName("정상셋은 여섯 규칙이 전부 0이다 — 본문이 비어 보이면 안 된다")
        void cleanRunShowsAllRulesAsZero() {
            VerifyReportView view = VerifyReportView.of(
                    run(DatasetType.CLEAN, null), Map.of(), null);

            assertThat(view.byType())
                    .hasSize(FindingType.values().length)
                    .allSatisfy((type, count) -> assertThat(count).isZero());
        }

        @Test
        @DisplayName("저장소가 null 을 줘도 여섯을 채운다")
        void toleratesNullCounts() {
            VerifyReportView view = VerifyReportView.of(
                    run(DatasetType.CLEAN, null), null, null);

            assertThat(view.byType()).hasSize(FindingType.values().length);
        }

        @Test
        @DisplayName("규칙 순서는 FindingType 선언 순서다 — 실행마다 같아야 diff 가 뜻을 갖는다")
        void keepsDeclarationOrder() {
            VerifyReportView view = VerifyReportView.of(
                    run(DatasetType.CLEAN, null), Map.of(), null);

            assertThat(view.byType().keySet())
                    .containsExactly(FindingType.values());
        }
    }

    @Nested
    @DisplayName("정답 대조")
    class ManifestSection {

        private static VerifyReportView.Manifest compared(
                List<FindingKey> missing, List<FindingKey> unexpected) {
            return VerifyReportView.Manifest.compared(11L, 800, "digest", missing, unexpected);
        }

        private static List<FindingKey> keys(int count) {
            List<FindingKey> keys = new java.util.ArrayList<>();
            for (int i = 0; i < count; i++) {
                keys.add(new FindingKey("STOCK_MISMATCH", "COUPON:%05d".formatted(i)));
            }
            return keys;
        }

        @Test
        @DisplayName("누락도 오탐도 없으면 일치다 — D10 게이트가 읽는 값")
        void matchesWhenBothEmpty() {
            assertThat(compared(List.of(), List.of()).matches()).isTrue();
        }

        @Test
        @DisplayName("한쪽만 비어도 일치가 아니다 — 한 방향만 보면 나머지를 못 본다")
        void doesNotMatchWhenOneSideHasRows() {
            FindingKey key = new FindingKey("STOCK_MISMATCH", "COUPON:1");

            assertThat(compared(List.of(key), List.of()).matches())
                    .as("누락만 있어도 불일치다")
                    .isFalse();
            assertThat(compared(List.of(), List.of(key)).matches())
                    .as("오탐만 있어도 불일치다")
                    .isFalse();
        }

        @Test
        @DisplayName("목록을 복사해 둔다 — 밖에서 바꿔도 리포트가 안 흔들린다")
        void copiesLists() {
            List<FindingKey> mutable = new java.util.ArrayList<>();
            mutable.add(new FindingKey("STOCK_MISMATCH", "COUPON:1"));

            VerifyReportView.Manifest manifest = compared(mutable, List.of());
            mutable.clear();

            assertThat(manifest.missing())
                    .as("제출물이 만들어진 뒤에 바뀌면 그것은 더 이상 그 시점의 사실이 아니다")
                    .hasSize(1);
        }

        @Test
        @DisplayName("null 목록은 빈 목록으로 받는다")
        void toleratesNullLists() {
            VerifyReportView.Manifest manifest = compared(null, null);

            assertThat(manifest.missing()).isEmpty();
            assertThat(manifest.unexpected()).isEmpty();
            assertThat(manifest.matches()).isTrue();
        }

        /**
         * <b>레코드의 canonical 생성자는 {@code public} 이다.</b> 지금은 팩토리 둘만 부르지만,
         * 캐시 계층이나 매퍼가 붙으면서 직접 부르는 날이 온다. 그때 null 이 들어오면
         * {@code missing.size()} 가 NPE 를 내므로, 방어가 <b>실제로 도는지</b> 여기서 잰다.
         */
        @Test
        @DisplayName("생성자에 null 목록을 직접 넘겨도 빈 목록이 된다 — 방어가 죽어 있지 않다")
        void canonicalConstructorTakesNullLists() {
            VerifyReportView.Manifest manifest = new VerifyReportView.Manifest(
                    true, 11L, 800, "d", 0, 0, null, null);

            assertThat(manifest.missing()).isEmpty();
            assertThat(manifest.unexpected()).isEmpty();
        }

        @Nested
        @DisplayName("대조를 못 한 상태")
        class NotCompared {

            /**
             * <b>빈 목록 둘을 "일치" 로 읽으면 안 된다.</b> 대조가 성공해서 빈 것이 아니라
             * <b>대조를 못 했기 때문</b>이다.
             *
             * <p>그렇다고 {@code false} 도 아니다 — 그것은 반대쪽 거짓말이라
             * {@code verdict=PASS} 옆에 {@code matches=false} 가 실린다.
             */
            @Test
            @DisplayName("matches 가 null 이다 — true 도 false 도 거짓말이다")
            void matchesIsUnknown() {
                VerifyReportView.Manifest manifest = VerifyReportView.Manifest.absent(11L);

                assertThat(manifest.present()).isFalse();
                assertThat(manifest.matches())
                        .as("게이트는 matches == true 로 읽는다. null 은 통과하지 않는다")
                        .isNull();
                assertThat(manifest.truncated())
                        .as("대조를 안 했는데 '잘렸다' 고 하면 안 된다")
                        .isFalse();
            }

            @Test
            @DisplayName("수치를 0으로 채우지 않는다 — 0은 '정답이 0건인 시드' 와 같은 값이다")
            void carriesNoNumbers() {
                VerifyReportView.Manifest manifest = VerifyReportView.Manifest.absent(11L);

                assertThat(manifest.expectedCount()).isNull();
                assertThat(manifest.expectedDigest()).isNull();
                assertThat(manifest.missingCount())
                        .as("0을 실으면 missingCount == 0 을 보는 쪽이 합격으로 읽는다")
                        .isNull();
                assertThat(manifest.unexpectedCount()).isNull();
                assertThat(manifest.missing()).isEmpty();
                assertThat(manifest.unexpected()).isEmpty();
            }

            @Test
            @DisplayName("대조를 안 했는데 결과가 실린 객체는 만들 수 없다")
            void rejectsResultsWithoutComparison() {
                assertThatThrownBy(() -> new VerifyReportView.Manifest(
                        false, 11L, 800, null, 0, 0, List.of(), List.of()))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("대조를 못 했는데");

                assertThatThrownBy(() -> new VerifyReportView.Manifest(
                        false, 11L, null, null, null, null,
                        List.of(new FindingKey("STOCK_MISMATCH", "COUPON:1")), List.of()))
                        .as("목록만 실려도 근거 없는 결과다")
                        .isInstanceOf(IllegalArgumentException.class);
            }

            @Test
            @DisplayName("대조를 했는데 수치가 비면 만들 수 없다 — 반대 방향도 막는다")
            void rejectsComparisonWithoutNumbers() {
                assertThatThrownBy(() -> new VerifyReportView.Manifest(
                        true, 11L, null, "d", null, null, List.of(), List.of()))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("수치가 다 있어야");
            }
        }

        @Nested
        @DisplayName("목록 크기")
        class Size {

            /**
             * <b>이 리포트는 매일 공개 저장소에 커밋된다.</b> {@code target_key} 포맷이
             * 어긋나면 누락·오탐이 통째로 뒤집혀 수천 행이 된다 — 그것을 그대로 실으면
             * 수 MB JSON 이 매일 쌓인다.
             */
            @Test
            @DisplayName("목록은 SAMPLE_LIMIT 에서 자른다 — 총수는 안 자른다")
            void capsListsButNotCounts() {
                int over = VerifyReportView.Manifest.SAMPLE_LIMIT + 37;

                VerifyReportView.Manifest manifest = compared(keys(over), keys(over));

                assertThat(manifest.missing()).hasSize(VerifyReportView.Manifest.SAMPLE_LIMIT);
                assertThat(manifest.unexpected()).hasSize(VerifyReportView.Manifest.SAMPLE_LIMIT);
                assertThat(manifest.missingCount())
                        .as("총수를 목록 길이로 접으면 '몇 건 틀렸나' 를 영영 못 본다")
                        .isEqualTo(over);
                assertThat(manifest.unexpectedCount()).isEqualTo(over);
            }

            @Test
            @DisplayName("자를 게 없으면 안 자르고 truncated 도 false 다")
            void doesNotFlagTruncationWhenItFits() {
                VerifyReportView.Manifest manifest =
                        compared(keys(VerifyReportView.Manifest.SAMPLE_LIMIT), List.of());

                assertThat(manifest.missing()).hasSize(VerifyReportView.Manifest.SAMPLE_LIMIT);
                assertThat(manifest.truncated())
                        .as("경계에서 켜지면 '잘렸다' 가 늘 켜져 뜻을 잃는다")
                        .isFalse();
            }

            @Test
            @DisplayName("한쪽만 넘쳐도 truncated 다")
            void flagsTruncationWhenEitherSideOverflows() {
                assertThat(compared(keys(VerifyReportView.Manifest.SAMPLE_LIMIT + 1), List.of())
                        .truncated()).isTrue();
                assertThat(compared(List.of(), keys(VerifyReportView.Manifest.SAMPLE_LIMIT + 1))
                        .truncated()).isTrue();
            }

            /**
             * <b>이것이 자르기의 핵심 위험이다.</b> 목록으로 판정하면 자르는 행위가
             * 판정을 바꾼다 — 잘라서 빈 목록이 되면 불합격이 합격으로 뒤집힌다.
             */
            @Test
            @DisplayName("총수로 판정한다 — 목록이 잘려도 불일치는 불일치다")
            void judgesOnCountsNotOnTruncatedLists() {
                VerifyReportView.Manifest manifest = compared(keys(5_000), keys(5_000));

                assertThat(manifest.matches())
                        .as("자른 목록으로 판정하면 자르는 행위가 합격을 만든다")
                        .isFalse();
            }

            @Test
            @DisplayName("총수가 목록보다 작으면 만들 수 없다 — 거짓 총수는 곧 거짓 판정이다")
            void rejectsCountSmallerThanList() {
                List<FindingKey> two = keys(2);

                assertThatThrownBy(() -> new VerifyReportView.Manifest(
                        true, 11L, 800, "d", 1, 0, two, List.of()))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("총수가 실린 목록보다 작습니다");
            }

            /**
             * 반대 방향. <b>자르지 않았는데 총수가 더 큰 것은 있을 수 없는 상태다.</b>
             * 그대로 두면 {@code truncated()} 가 true 를 내는데, 그것이 자르기 때문인지
             * 데이터가 샌 것인지 구분이 안 된다.
             */
            @Test
            @DisplayName("자르지 않았는데 총수가 더 크면 만들 수 없다")
            void rejectsCountLargerThanUntruncatedList() {
                assertThatThrownBy(() -> new VerifyReportView.Manifest(
                        true, 11L, 800, "d", 5_000, 0, keys(3), List.of()))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("잘리지 않았는데");
            }

            @Test
            @DisplayName("표본은 앞에서 자른다 — 저장소가 준 순서 그대로여야 diff 가 산다")
            void keepsRepositoryOrderWhenSampling() {
                List<FindingKey> all = keys(VerifyReportView.Manifest.SAMPLE_LIMIT + 10);

                assertThat(compared(all, List.of()).missing())
                        .as("무작위로 고르면 같은 판정을 두 번 떠도 다른 표본이 실린다")
                        .containsExactlyElementsOf(
                                all.subList(0, VerifyReportView.Manifest.SAMPLE_LIMIT));
            }

            @Test
            @DisplayName("자르기 한계를 본문에 싣는다 — 상수를 바꾼 날 diff 가 뜻을 잃지 않게")
            void carriesItsOwnSampleLimit() {
                assertThat(compared(List.of(), List.of()).sampleLimit())
                        .isEqualTo(VerifyReportView.Manifest.SAMPLE_LIMIT);
                assertThat(VerifyReportView.Manifest.absent(11L).sampleLimit())
                        .as("대조를 못 해도 리포터의 규칙 자체는 말할 수 있다")
                        .isEqualTo(VerifyReportView.Manifest.SAMPLE_LIMIT);
            }
        }
    }

    @Test
    @DisplayName("실행이 없으면 리포트를 만들 수 없다")
    void rejectsNullRun() {
        assertThatThrownBy(() -> VerifyReportView.of(null, Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("검증 실행");
    }

    @Test
    @DisplayName("run 을 통째로 싣는다 — 펴면 VerifyRunView 와 열넷이 겹친다")
    void carriesTheRunItself() {
        VerificationRun run = run(DatasetType.CORRUPT, 11L);

        assertThat(VerifyReportView.of(run, Map.of(), null).run())
                .as("같은 것을 두 군데서 관리하지 않는다는 것이 이 모양의 이유다")
                .isSameAs(run);
    }
}
