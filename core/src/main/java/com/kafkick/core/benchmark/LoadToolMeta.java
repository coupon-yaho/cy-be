package com.kafkick.core.benchmark;

/**
 * 부하를 만든 도구. <b>도구 이름이 사는 유일한 자리다.</b>
 *
 * <p>결과 컬럼 접두어를 도구 이름으로 짓지 않는 이유가 여기 있다 — {@code k6_p99} 처럼 스키마에
 * 도구명을 박으면 도구를 바꾸는 순간 컬럼명이 거짓이 되는데, 이미 쌓인 회차의 컬럼명은 고칠 수
 * 없다(측정은 되돌릴 수 없다). 그래서 결과는 {@link ClientLoadSummary} · {@link ServerLoadSummary}
 * 로 <b>관측 위치</b>에 따라 나누고, 도구가 무엇이었는지는 이 메타가 회차마다 따로 기록한다.
 *
 * <p>세 값이 다 없을 수 있다(수동 회차). 있으면 회차 간 비교의 전제를 검증하는 데 쓴다 —
 * 같은 {@code scenarioCode} 라도 {@code scriptHash} 가 다르면 같은 부하가 아니다.
 *
 * @param tool 도구 이름
 * @param version 도구 버전. 같은 도구라도 버전에 따라 백분위 산식이 달라질 수 있다
 * @param scriptHash 부하 스크립트의 SHA-256
 */
public record LoadToolMeta(String tool, String version, String scriptHash) {

    private static final LoadToolMeta UNKNOWN = new LoadToolMeta(null, null, null);

    /**
     * 도구 정보를 남기지 않은 회차.
     *
     * @return 세 값이 모두 비어 있는 메타
     */
    public static LoadToolMeta unknown() {
        return UNKNOWN;
    }
}
