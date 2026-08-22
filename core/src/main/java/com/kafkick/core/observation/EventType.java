package com.kafkick.core.observation;

/**
 * HTTP 진입부터 발급 결과까지의 운영 관측 단계입니다.
 * 발급권 상태 전이를 나타내는 {@code IssuanceEventType}과는 별도 계약입니다.
 *
 * <p>값은 4종입니다. {@code V2026082001__issue_attempts.sql}의 {@code event_type} 주석은
 * 3종으로 남아 있습니다 — 이미 적용된 마이그레이션 파일은 주석도 checksum에 들어가 수정할 수
 * 없습니다. 새 마이그레이션의 {@code ALTER TABLE ... MODIFY ... COMMENT}로 DB 주석을 갱신하는
 * 길은 열려 있지만 이 티켓의 범위 밖입니다.
 * TODO(OBS-25): DB 주석을 4종으로 갱신하는 마이그레이션을 함께 낸다.
 * 그때까지 종수의 최신 사실은 이 javadoc이 원본이고, DB 주석은 뒤처져 있습니다.
 *
 * <p>{@link #ISSUE_ATTEMPT}는 {@code replayed=true}인 재시도도 그대로 셉니다. 같은 요청이 여러 번
 * 시도되면 그만큼 값이 오릅니다 — 바로 앞의 {@link #QUEUE_ADMITTED}가 재시도를 거부하는 것과
 * 규칙이 다릅니다. 입장 허용은 요청당 한 번뿐인 사건이지만 시도는 반복되는 단계이기 때문입니다.
 * 따라서 <b>성공률의 분모로 쓰면 안 됩니다</b>. 정책 검증 통과분만 세므로 재시도가 없어도
 * {@code sum(outcome) > attempt}이고, 재시도가 몰리면 반대 방향으로도 어긋납니다. O1에서는
 * 무트래픽과 발급 중단을 가르는 값으로만 씁니다.
 *
 * <p>{@link #ISSUE_ATTEMPT}는 결과가 아니라 단계입니다. 발급 결과 이벤트에서 파생할 수 없어
 * 별도 값으로 둡니다 — 결과로 세면 처리 중 멈춘 요청이 통째로 빠지고, 그 누락은 재고 소진 직전에
 * 가장 커서 "수요가 없었다"는 반대 결론을 만듭니다.
 */
public enum EventType {

    ENTRY_RESULT,
    QUEUE_ADMITTED,
    ISSUE_ATTEMPT,
    ISSUE_RESULT
}
