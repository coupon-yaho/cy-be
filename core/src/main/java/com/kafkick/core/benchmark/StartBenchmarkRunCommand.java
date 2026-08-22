package com.kafkick.core.benchmark;

import java.util.Objects;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;

/**
 * 회차를 여는 명령. 회차의 조건은 여기서 <b>전부</b> 고정된다.
 *
 * <p>시작 시점에 다 받는 이유는 측정이 되돌릴 수 없기 때문이다. 회차가 끝난 뒤에 "이때 worker 가
 * 몇이었더라" 를 채워 넣을 수는 있지만, 그건 기억이지 관측이 아니다.
 *
 * @param runKey 회차 키. 같은 키로 두 번 열 수 없다
 * @param runType 회차 성격
 * @param scenarioCode 부하 시나리오 코드
 * @param engineVersion 이 회차의 권위 엔진 버전
 * @param releaseStage 릴리스 단계
 * @param queueMode 대기열 모드
 * @param couponId 이 회차가 쓸 쿠폰. 회차마다 새로 만들므로 재고 리셋 절차가 없다
 * @param requestedBy 회차를 여는 사람
 * @param topology 자원·토폴로지 조건
 * @param loadProfile 부하 입력
 * @param toolMeta 부하 도구 메타. 모르면 {@link LoadToolMeta#unknown()}
 */
public record StartBenchmarkRunCommand(
        String runKey,
        BenchmarkRunType runType,
        String scenarioCode,
        EngineVersion engineVersion,
        ReleaseStage releaseStage,
        QueueMode queueMode,
        Long couponId,
        String requestedBy,
        BenchmarkTopology topology,
        LoadProfile loadProfile,
        LoadToolMeta toolMeta
) {

    public StartBenchmarkRunCommand {
        runKey = requireText(runKey, "runKey", 64);
        scenarioCode = requireText(scenarioCode, "scenarioCode", 64);
        requestedBy = requireText(requestedBy, "requestedBy", 100);
        Objects.requireNonNull(runType, "runType");
        Objects.requireNonNull(engineVersion, "engineVersion");
        Objects.requireNonNull(releaseStage, "releaseStage");
        Objects.requireNonNull(queueMode, "queueMode");
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(loadProfile, "loadProfile");
        Objects.requireNonNull(toolMeta, "toolMeta");
    }

    /**
     * 길이를 여기서 자른다. 컬럼 길이를 넘기면 MySQL strict mode 가 1406 으로 거부하는데,
     * 그 시점은 이미 회차를 시작한 뒤라 사람이 다시 입력해야 한다.
     */
    private static String requireText(String value, String name, int maxLength) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " 를 입력해야 한다");
        }
        String trimmed = value.strip();
        if (trimmed.length() > maxLength) {
            throw new IllegalArgumentException(
                    name + " 는 " + maxLength + "자를 넘을 수 없다: " + trimmed.length());
        }
        return trimmed;
    }
}
