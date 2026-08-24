// 되읽기가 배치 메타에서 마지막 성공을 찾는 창입니다. 보존 하한이 여기서 파생됩니다.
package com.kafkick.batch.config;

/**
 * <b>되읽기 둘과 보존 하한이 함께 아는 값 하나.</b>
 *
 * <p>{@code BatchRunMetricsRefresher} 와 {@code ExpirePendingRefresher} 가 <b>마지막 성공
 * 실행</b>을 이 창 안에서 찾고, {@code CleanupJobConfig} 의 배치 메타 보존 하한이 여기서
 * 파생된다({@code LOOKBACK_DAYS + 1}).
 *
 * <h2>왜 클래스를 따로 두나</h2>
 *
 * <p><b>그전에는 같은 7이 세 곳에 따로 박혀 있었다</b> — 두 되읽기의 {@code INTERVAL 7 DAY}
 * 리터럴과 {@code CleanupJobConfig} 의 상수. 셋을 잇는 것이 코드에 없어 한쪽만 고쳐도
 * 전부 초록이었고, {@code RefreshWindowLockTest} 가 <b>소스를 정규식으로 읽어</b> 그 간극을
 * 메우고 있었다. 이제 리터럴이 이 상수에서 나오므로 어긋날 자리가 코드에 없다(CY-470).
 *
 * <p><b>{@code CleanupJobConfig} 에 두면 안 된다.</b> 그러면
 * {@code batch.config → batch.job} 화살표가 생기는데 그 방향은 이 저장소에 없다
 * ({@code BatchStructuralContractTest} 가 자물쇠다). 창의 <b>주인은 되읽기</b>이고
 * 보존 하한이 그것에서 파생되는 것이지 그 반대가 아니라, 방향과 소유가 여기서 일치한다.
 *
 * <p><b>{@code VerifyRunContext} 에도 두면 안 된다.</b> 만료 되읽기도 이 창을 쓴다 —
 * 검증 계약에 얹으면 이름이 내용을 속인다.
 *
 * <p>{@code ExpireStepContext} 와 같은 규율이다: 계약은 계약이 있을 자리에 둔다.
 */
public final class BatchMetadataWindow {

    /**
     * 되읽기가 <b>마지막 성공 실행</b>을 찾는 창(일).
     *
     * <p><b>설정으로 빼지 않는다.</b> 이 창은 알림 식·보존 기간과 함께 성립해야 하는
     * <b>불변식의 한 항</b>이지 환경마다 다른 손잡이가 아니다. 열면 한쪽만 움직일 수 있게
     * 되고, 그것이 이 상수가 없애려는 상태다.
     *
     * <p>⚠️ <b>SLA 보다 넉넉히 커야 한다.</b> 창 안에 성공이 없으면 게이지가 {@code NaN} 이
     * 되는데, 그것은 <i>"오래 안 돌았다"</i> 가 아니라 <i>"한 번도 안 돌았다"</i> 로 보고된다 —
     * 사고 등급이 바뀐다.
     *
     * <p>⚠️ <b>넓히면 보존 하한이 따라 올라간다</b>({@code MIN_METADATA_KEEP_DAYS}).
     * 안 따라 올리면 <b>창 안(예: 9~14일 전)의 마지막 성공이 보존 삭제로 지워지는데</b>,
     * 그 사실은 <b>잡이 보존 기간 넘게 연속 실패한 날에야</b> 드러난다.
     * 지금은 두 값이 한 상수에서 나오므로 그 자리가 없다.
     */
    public static final int LOOKBACK_DAYS = 7;

    private BatchMetadataWindow() {
    }
}
