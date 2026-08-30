// 검증 실행 하나의 결과 전체입니다. D13 제출물이 이 응답을 그대로 떠서 커밋합니다.
package com.kafkick.batch.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.kafkick.core.verification.FindingKey;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.VerificationRun;

/**
 * <b>D13 제출물이다.</b> {@code docs/10} 이 리포트를 <i>"두 얼굴"</i> 로 갈랐다 —
 * 개발용은 <b>배치 실행</b>이 어떻게 됐나({@link VerifyRunView}), 이쪽은 <b>그 실행이 낸
 * 판정</b>이다.
 *
 * <h2>{@link VerifyRunView} 와 무엇이 다른가</h2>
 *
 * <p>겹치는 것이 많아 보이지만 <b>주소가 다르다.</b> 그쪽은 {@code executionId} 로 찾고
 * {@code status}·{@code failure} 처럼 <b>배치 실행에만 있는 것</b>을 싣는다. 이쪽은
 * {@code dataset}+{@code scope} 의 <b>마지막 판정</b>을 찾는다 — 제출물이 필요로 하는 것이
 * <i>"어느 실행이 어떻게 끝났나"</i> 가 아니라 <i>"최종 결과가 무엇인가"</i> 라서다.
 *
 * <p><b>필드를 펴지 않고 {@code run} 을 통째로 싣는다.</b> 펴면 열넷이
 * {@link VerifyRunView} 와 이름이 겹치고, 그 순간 <b>같은 것을 두 군데서 관리</b>하게 된다.
 * 통째로 실으면 {@link VerificationRun} 이 바뀔 때 여기가 자동으로 따라간다.
 *
 * <h2>PII</h2>
 *
 * <p>{@code PRD:2143} 이 <i>"집계값만. {@code member_id} 는 남기되 이름·연락처 금지"</i> 로
 * 정했다.
 *
 * <p><b>{@code member_id} 는 실린다.</b> {@code DUP_PER_MEMBER} 의 {@code target_key} 가
 * {@code COUPON:{id}|MEMBER:{id}} 라서다 — 그것이 <b>허용된 범위</b>다.
 * {@code PRD:385} 가 이유를 적었다: <i>"마스킹하면 검증 리포트에서 어느 회원이 중복
 * 발급됐는지를 쓸 수 없습니다."</i>
 *
 * <h3>매일 공개 저장소에 커밋된다 — 그것까지 보고 남긴 결정이다</h3>
 *
 * <p>{@code scripts/dump-verify-report.sh} 가 이 응답을 <b>매일 공개 저장소에 커밋한다.</b>
 * 그러면 노출 성격이 <i>"API 를 쳐야 보이는 값"</i> 에서 <i>"검색되고 지울 수 없는 기록"</i>
 * 으로 바뀐다. 그 차이를 알고 그대로 두기로 했다. 근거 셋:
 *
 * <ol>
 *   <li><b>실제 사용자가 아니다.</b> 검증이 도는 두 데이터셋({@code CLEAN}·{@code CORRUPT})은
 *       전부 시드가 생성한 회원이다 — 유저 100만·이력 300만이 그것이다.</li>
 *   <li><b>합격한 날에는 아예 안 실린다.</b> 두 목록이 비기 때문이다.
 *       {@code member_id} 가 나가는 것은 <b>검증이 실패한 날</b>뿐이다.</li>
 *   <li><b>실려도 {@link Manifest#SAMPLE_LIMIT} 개까지다.</b> 전량 덤프가 아니다.</li>
 * </ol>
 *
 * <p><b>실 데이터로 이 조회를 돌리게 되면 이 결정을 다시 봐야 한다.</b> 위 셋 중 첫째가
 * 무너지고, 나머지 둘은 노출을 줄일 뿐 없애지 않는다.
 *
 * <p>한때 여기 <i>"이 뷰는 {@code members} 를 아예 모른다 — 조인할 수단 자체가 없다"</i> 고
 * 적혀 있었는데 <b>거짓이었다.</b> {@code target_key} 안의 {@code member_id} 가 곧 조인 키다.
 * 이름·연락처가 없는 진짜 이유는 <b>이 경로가 {@code members} 를 조회하지 않기 때문</b>이고,
 * 그것은 구조가 아니라 <b>지켜야 하는 규율</b>이다 — "구조적으로 불가능" 이라고 적어 두면
 * 다음 사람이 이 뷰에 PII 검사를 안 한다.
 *
 * @param schema   이 배치가 붙어 있는 데이터베이스 이름. <b>{@code dataset} 만으로는
 *                 화면이 카드를 못 가른다</b> — 정상셋 배치와 운영 배치가 둘 다
 *                 {@code CLEAN} 이라, 이름표가 같은 카드 두 장이 된다. 그때 앞선 것을
 *                 중복으로 버리면 <b>다른 데이터가 조용히 사라진다</b>(cy-fe 가 겪었다).
 *                 {@code dataset} 은 <i>"어떤 종류를 검증하나"</i> 이고 이 값이
 *                 <i>"어느 DB 를 보나"</i> 다 — 둘은 다른 축이라 하나로 못 접는다.
 * @param run      판정이 담긴 실행. {@code finished_at} 이 없는 행은 판정이 아니다
 * @param byType   규칙별 검출 수. <b>검출이 0인 규칙도 들어 있다</b> — 아래 {@code of} 참고
 * @param manifest 오염셋 대조. 정상셋이면 {@code null} 이다 — 대조할 정답이 없다
 */
@com.fasterxml.jackson.annotation.JsonPropertyOrder({"schema", "run", "byType", "manifest"})
public record VerifyReportView(
        String schema,
        VerificationRun run,
        Map<FindingType, Integer> byType,
        Manifest manifest
) {

    /**
     * <b>검출이 0인 규칙도 채워서 준다.</b> {@code GROUP BY} 는 없는 것을 못 만들어서
     * 저장소는 검출이 있는 규칙만 준다. 그대로 내보내면 <i>"그 규칙이 0건이다"</i> 와
     * <i>"그 규칙을 안 돌렸다"</i> 가 응답에서 같은 모양이 된다 — 정상셋은 여섯 규칙이 전부
     * 0건이라 <b>본문이 통째로 비어 보인다.</b>
     *
     * <p>규칙 목록의 주인은 {@link FindingType} 이므로 채우는 것도 여기서 한다.
     */
    public static VerifyReportView of(String schema, VerificationRun run,
            Map<FindingType, Integer> counted, Manifest manifest) {
        if (run == null) {
            throw new IllegalArgumentException("리포트를 만들 검증 실행이 필요합니다.");
        }
        if (schema == null || schema.isBlank()) {
            // 비어 있으면 화면이 카드를 못 가른다. 빈 값을 실어 보내면 "이름이 없다" 가
            // "이름이 같다" 와 한 모양이 되어, 다른 데이터가 중복으로 버려진다.
            throw new IllegalArgumentException("리포트에 실을 스키마 이름이 없습니다.");
        }

        Map<FindingType, Integer> filled = new LinkedHashMap<>();
        for (FindingType type : FindingType.values()) {
            filled.put(type, counted == null ? 0 : counted.getOrDefault(type, 0));
        }
        // **Map.copyOf 를 쓰면 안 된다.** 그것은 순서를 보장하지 않는다 — 제출물이 커밋돼
        // diff 되는데 규칙 순서가 실행마다 달라지면 "결과가 바뀐 것" 으로 읽힌다.
        return new VerifyReportView(schema, run, Collections.unmodifiableMap(filled), manifest);
    }

    /**
     * 시드가 심은 정답과의 <b>양방향</b> 대조. 한 방향만 보면 누락과 오탐 중 하나를 못 본다.
     *
     * <p><b>{@code present} 가 먼저다.</b> 정답 묶음이 사라졌으면(시드 재주입 등) 양방향
     * 대조가 성립하지 않는다 — {@code expected_findings} 가 0행이면 {@code LEFT JOIN} 이
     * <b>검출 전부를 오탐으로 뒤집는다.</b> 그 상태로 목록을 실으면
     * <i>"검증기가 800건을 오탐했다"</i> 는 제출물이 나가고, {@code verdict} 와 정면으로
     * 모순된다. {@code ExpectedFindingRepository.unexpected} 의 javadoc 이 그 함정을 적어 뒀다.
     *
     * <p><b>죽이지 않고 싣는 것이 결정이다.</b> 검증 잡은 같은 상황을 {@code MANIFEST_ABSENT}
     * 로 죽인다 — 그쪽은 판정을 만드는 중이라 잘못된 판정을 남기면 안 되기 때문이다.
     * 이쪽은 <b>이미 난 판정을 읽을 뿐</b>이라, 500 을 내면 제출물을 뜨려는 사람이
     * 판정 자체를 못 본다. 대조만 접고 나머지는 그대로 준다.
     *
     * <h3>대조를 못 한 상태에는 값을 채우지 않는다</h3>
     *
     * <p>{@code present=false} 면 <b>수치 넷이 전부 {@code null}</b> 이다. 한때 0 을 채웠는데
     * 그것이 거짓이었다 — {@code expectedCount=0} 은 <i>"정답 묶음이 사라졌다"</i> 와
     * <i>"정답이 0건인 시드다"</i> 를 같은 값으로 뭉치고, {@code missingCount=0} 을 보는 쪽은
     * <b>그것을 합격으로 읽는다.</b> 없는 값은 없는 채로 나가야 한다. 아래 compact 생성자가
     * 그 규칙을 구조로 박는다 — 대조를 안 했는데 결과가 실린 객체는 만들 수 없다.
     *
     * <h3>목록은 표본이다 — 판정은 총수가 한다</h3>
     *
     * <p>불일치가 나면 두 목록이 <b>검출 전부만큼 커진다.</b> 오염셋 정답이 800행이고
     * 검출은 규칙이 어긋나면 그보다 훨씬 많아진다 — {@code target_key} 포맷이 한 글자
     * 틀리면 <b>양쪽이 통째로 어긋나</b> 누락 800 + 오탐 N 이 된다. 그것을 그대로 실으면
     * <b>공개 저장소에 수 MB JSON 이 매일 커밋된다.</b>
     *
     * <p>그래서 목록은 {@link #SAMPLE_LIMIT} 개까지만 싣고, <b>판정은 잘리지 않는
     * {@code missingCount}·{@code unexpectedCount} 가 한다.</b> 목록으로 판정하면
     * 201번째부터가 사라진 순간 {@code matches} 가 뒤집힌다 — 잘라서 합격시키는 셈이다.
     *
     * <p>{@code truncated} 와 {@code sampleLimit} 이 함께 실린다. {@code truncated} 없이
     * 200건짜리 목록을 보면 <i>"정확히 200건 틀렸다"</i> 로 읽히고, {@code sampleLimit} 없이
     * 상수를 200에서 50으로 바꾸면 <b>판정이 그대로인데 전 파일의 바이트가 달라진다</b> —
     * diff 에서 <i>"결과가 크게 바뀌었다"</i> 와 구분되지 않는다. 커밋되는 파일은 자기
     * 자르기 규칙을 스스로 말해야 한다.
     *
     * @param present         정답 묶음이 실제로 있나. <b>{@code false} 면 아래 수치가 전부 null</b>
     * @param seedRunId       대조한 시드 실행
     * @param expectedCount   정답 행수. <b>위반</b>의 수다
     * @param corruptionCount 심은 <b>오염</b>의 수. 위와 다르다 — 오염 하나가 규칙 여럿을
     *                        어길 수 있어서, 지금 시드에서는 오염 700 이 위반 800 을 낳는다.
     *                        이 값이 없으면 화면이 700 을 추정해야 하고, 그 추정은 시드가
     *                        오염 종류를 하나 더 심는 날 조용히 틀린다
     * @param missingCount    정답에 있는데 못 잡은 것의 <b>총수</b>. 0건이 합격
     * @param unexpectedCount 잡았는데 정답에 없는 것의 <b>총수</b>. 0건이 합격
     * @param missing         위 총수의 앞 {@link #SAMPLE_LIMIT} 개. 판정용이 아니라 진단용
     * @param unexpected      위 총수의 앞 {@link #SAMPLE_LIMIT} 개. 판정용이 아니라 진단용
     */
    @com.fasterxml.jackson.annotation.JsonPropertyOrder({
            "present", "seedRunId", "sampleLimit", "expectedCount", "corruptionCount",
            "expectedDigest",
            "missingCount", "unexpectedCount", "matches", "truncated", "missing", "unexpected"})
    public record Manifest(boolean present, long seedRunId, Integer expectedCount,
            Integer corruptionCount,
            String expectedDigest, Integer missingCount, Integer unexpectedCount,
            List<FindingKey> missing, List<FindingKey> unexpected) {

        /**
         * 목록에 실을 최대 개수.
         *
         * <p><b>순서가 있어야 표본이 뜻을 갖는다.</b> {@code ExpectedFindingJdbcAdapter} 가
         * {@code ORDER BY CAST(finding_type AS BINARY), CAST(target_key AS BINARY)} 로
         * 준다 — 정렬이 없으면 같은 판정을 두 번 떠도 <b>다른 200건</b>이 실려 diff 가
         * 뜻을 잃는다. 그 정렬에 <b>동률이 없다</b>는 것도 확인했다:
         * {@code uk_expected}·{@code uk_run_finding} 이 정렬 키 둘을 유일하게 만든다.
         */
        public static final int SAMPLE_LIMIT = 200;

        public Manifest {
            missing = missing == null ? List.of() : List.copyOf(missing);
            unexpected = unexpected == null ? List.of() : List.copyOf(unexpected);

            if (present) {
                if (expectedCount == null || missingCount == null || unexpectedCount == null
                        || corruptionCount == null) {
                    throw new IllegalArgumentException(
                            ("대조를 했으면 수치가 다 있어야 합니다: "
                                    + "expected=%s corruption=%s missing=%s unexpected=%s")
                                    .formatted(expectedCount, corruptionCount,
                                            missingCount, unexpectedCount));
                }
                checkSample("missing", missingCount, missing.size());
                checkSample("unexpected", unexpectedCount, unexpected.size());
            } else if (expectedCount != null || missingCount != null || unexpectedCount != null
                    || corruptionCount != null || expectedDigest != null
                    || !missing.isEmpty() || !unexpected.isEmpty()) {
                // 대조를 못 했는데 결과가 실려 있으면 그 결과는 근거가 없다. 만들 수 없게 한다.
                throw new IllegalArgumentException("대조를 못 했는데 대조 결과가 실려 있습니다.");
            }
        }

        /**
         * 총수와 표본이 <b>구조적으로 가능한 조합</b>인지 본다.
         *
         * <p>총수는 {@code matches()} 가 읽는 값이라 <b>거짓 총수는 곧 거짓 판정</b>이다.
         * 조용히 고치면 그 거짓이 제출물로 나간다.
         *
         * <p>두 방향을 다 막는다. 총수가 목록보다 <b>작으면</b> 총수가 거짓이고,
         * 총수가 목록보다 <b>큰데 목록이 표본 한계에 안 닿았으면</b> 그것도 거짓이다 —
         * 자르지 않았는데 뭔가 사라졌다는 뜻이라 있을 수 없는 상태다.
         */
        private static void checkSample(String name, int count, int size) {
            if (count < size) {
                throw new IllegalArgumentException(
                        "%s 총수가 실린 목록보다 작습니다: %d < %d".formatted(name, count, size));
            }
            if (count > size && size != SAMPLE_LIMIT) {
                throw new IllegalArgumentException(
                        "%s 가 잘리지 않았는데 총수가 더 큽니다: %d > %d (한계 %d)"
                                .formatted(name, count, size, SAMPLE_LIMIT));
            }
        }

        /**
         * 대조 결과 전체를 받아 <b>총수는 세고 목록은 자른다.</b>
         *
         * <p>부르는 쪽이 자르면 총수와 목록이 어긋날 수 있다. 자르는 자리를 여기 하나로
         * 두면 그 어긋남이 구조적으로 안 생긴다.
         */
        public static Manifest compared(long seedRunId, int expectedCount,
                int corruptionCount, String expectedDigest, List<FindingKey> missing,
                List<FindingKey> unexpected) {
            List<FindingKey> allMissing = missing == null ? List.of() : missing;
            List<FindingKey> allUnexpected = unexpected == null ? List.of() : unexpected;
            return new Manifest(true, seedRunId, expectedCount, corruptionCount, expectedDigest,
                    allMissing.size(), allUnexpected.size(),
                    sample(allMissing), sample(allUnexpected));
        }

        private static List<FindingKey> sample(List<FindingKey> all) {
            return all.size() <= SAMPLE_LIMIT ? List.copyOf(all)
                    : List.copyOf(all.subList(0, SAMPLE_LIMIT));
        }

        /** 정답 묶음이 사라진 상태. 대조 결과 대신 <b>그 사실만</b> 싣는다. */
        public static Manifest absent(long seedRunId) {
            return new Manifest(false, seedRunId, null, null, null, null, null,
                    List.of(), List.of());
        }

        /**
         * 집합이 정확히 일치하는가. <b>D10 게이트가 읽는 값이다.</b>
         *
         * <p><b>{@code @JsonProperty} 가 없으면 응답에 안 실린다.</b> 레코드의 파생 메서드는
         * 컴포넌트가 아니라 Jackson 이 안 본다.
         *
         * <p><b>세 값이다 — {@code true} · {@code false} · {@code null}.</b> 한때
         * 대조를 못 한 상태를 {@code false} 로 접었는데, 그것이 <b>반대쪽 거짓말</b>이었다:
         * 정답 묶음이 사라진 것과 검증기가 틀린 것이 한 값이 되고, 제출물이
         * {@code verdict=PASS} 와 {@code matches=false} 를 <b>같은 본문에</b> 싣는다.
         * 그것을 보는 사람은 어느 쪽을 믿을지 알 수 없다.
         *
         * <p>그래서 대조를 못 했으면 {@code null} 이다 — <i>"못 쟀다"</i>. 게이트는
         * {@code matches == true} 로 읽어야 하고, 그러면 {@code null} 도 {@code false} 도
         * 통과하지 않는다(둘 다 안전한 방향). 대신 그 <b>이유</b>가 {@code present} 에 남는다.
         *
         * <p><b>목록이 아니라 총수로 판정한다.</b> 목록은 잘린다 — 잘린 목록이 비었다고
         * 합격시키면 자르는 행위가 판정을 바꾼다.
         */
        @com.fasterxml.jackson.annotation.JsonProperty("matches")
        public Boolean matches() {
            return present ? missingCount == 0 && unexpectedCount == 0 : null;
        }

        /** 목록이 잘렸나. 없으면 표본 200건이 <i>"정확히 200건 틀렸다"</i> 로 읽힌다. */
        @com.fasterxml.jackson.annotation.JsonProperty("truncated")
        public boolean truncated() {
            return present
                    && (missingCount > missing.size() || unexpectedCount > unexpected.size());
        }

        /**
         * 자르기 한계를 <b>본문에 실어</b> 파일이 자기 규칙을 스스로 말하게 한다.
         *
         * <p>이것이 없으면 상수를 200에서 50으로 바꾼 날, 판정이 그대로인데 전 조합의
         * 파일이 한꺼번에 바뀐 커밋이 올라간다 — <i>"그날 결과가 크게 바뀌었다"</i> 로 읽힌다.
         */
        @com.fasterxml.jackson.annotation.JsonProperty("sampleLimit")
        public int sampleLimit() {
            return SAMPLE_LIMIT;
        }
    }
}
