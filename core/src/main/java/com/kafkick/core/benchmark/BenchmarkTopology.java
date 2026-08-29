package com.kafkick.core.benchmark;

import com.kafkick.core.support.exception.BusinessException;

/**
 * 회차가 돌던 자원 조건. <b>기록이 없으면 나중에 "이 수치가 어떤 조건에서 나왔는지" 재현이 안 된다.</b>
 *
 * <p>전부 <b>총량</b>이다. 인스턴스별 값을 담지 않는다(DEC-08) — Prometheus 가 scrape 때 이미
 * {@code instance} 라벨을 붙이므로 인스턴스별 비교는 그쪽 경로로 하고, 여기는 회차 전체의
 * 조건만 담는다. 앱이 인스턴스 이름표를 또 붙이면 이름표가 두 개가 되고 시계열 수가 그만큼 는다.
 *
 * @param appReplicas api 인스턴스 수 N
 * @param availableProcessors {@code Runtime.getRuntime().availableProcessors()} 실측값(DEC-07).
 *        {@code -XX:ActiveProcessorCount} 로 가정을 설정에 박는 대신 실제 값을 관측해 기록한다 —
 *        인스턴스를 한 머신에 몰아넣는 구성으로 바뀌어도 그 사실이 데이터에 남는다
 * @param cpuMillicoresTotal CPU 총량. {@code compose.yml} 에 {@code cpus} 제한이 한 줄도 없어(실측)
 *        지금은 강제되지 않으므로 모를 수 있다
 * @param memoryMbTotal 메모리 총량. 위와 같은 이유로 모를 수 있다
 * @param tomcatWorkersTotal worker 총량
 * @param tomcatMaxConnections 2만 연결이 1초에 들이닥칠 때 worker 보다 <b>먼저</b> 받는 관문.
 *        여기서 잘리면 세 버전이 전부 똑같이 소켓 거부만 내고 끝나 구분이 안 된다
 * @param tomcatAcceptCount 위와 짝이 되는 백로그 길이. 회차 전에 확정하고 기록한다
 * @param hikariPoolTotal 커넥션 풀 총량. offered RPS 와 무관하다 — 초과분은 풀에서 대기하고,
 *        그 대기가 쌓이는 걸 보는 게 v1 측정의 목적이다
 * @param mysqlMaxConnections 서버 상한. N=3 이면 여유가 3뿐이라 회차 중 수동 접속이 금지다
 */
public record BenchmarkTopology(
        int appReplicas,
        int availableProcessors,
        Integer cpuMillicoresTotal,
        Integer memoryMbTotal,
        int tomcatWorkersTotal,
        Integer tomcatMaxConnections,
        Integer tomcatAcceptCount,
        int hikariPoolTotal,
        int mysqlMaxConnections
) {

    public BenchmarkTopology {
        requirePositive(appReplicas, "appReplicas");
        requirePositive(availableProcessors, "availableProcessors");
        requirePositive(tomcatWorkersTotal, "tomcatWorkersTotal");
        requirePositive(hikariPoolTotal, "hikariPoolTotal");
        requirePositive(mysqlMaxConnections, "mysqlMaxConnections");
        requirePositiveIfPresent(cpuMillicoresTotal, "cpuMillicoresTotal");
        requirePositiveIfPresent(memoryMbTotal, "memoryMbTotal");
        requirePositiveIfPresent(tomcatMaxConnections, "tomcatMaxConnections");
        if (tomcatAcceptCount != null && tomcatAcceptCount < 0) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,"tomcatAcceptCount 는 0 이상이어야 한다: " + tomcatAcceptCount);
        }
    }

    /**
     * 이 JVM 이 실제로 보고 있는 코어 수. 회차를 여는 인스턴스에서 호출해 스냅샷에 싣는다.
     *
     * <p>이 값을 안 남기면 "이 회차는 몇 코어였지" 를 나중에 알 방법이 없다.
     *
     * @return 이 JVM 이 보는 가용 프로세서 수
     */
    public static int currentAvailableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,name + " 는 0 보다 커야 한다: " + value);
        }
    }

    private static void requirePositiveIfPresent(Integer value, String name) {
        if (value != null && value <= 0) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,name + " 는 0 보다 커야 한다: " + value);
        }
    }
}
