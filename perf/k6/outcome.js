// 응답 하나를 **네 갈래 중 하나**로 가른다. 이 판정이 독립 기록과 지표를 함께 정한다.
//
// 별도 모듈인 이유는 하나다 — issue.js 는 모듈 수준에서 __ENV 를 읽고 없으면 죽는다.
// 여기 두면 서버도 환경변수도 없이 k6 로 곧바로 태울 수 있다(perf/k6/outcome_test.js).

export const OK = 'OK';               // 서버가 받았다고 말했다
export const REJECTED = 'REJECTED';   // 서버가 안 받았다고 **말했다**
export const DEFERRED = 'DEFERRED';   // 서버가 **아직 안 정했다** — 기다리면 바뀐다
export const UNKNOWN = 'UNKNOWN';     // 서버가 뭘 했는지 모른다

/**
 * ⚠️ **상태 코드만으로는 못 가른다.**
 *
 * `409 COUPON-320` 은 "요청을 처리 중입니다" 이고 판정이 아니다. 그것을 거절로 세면
 * 대조가 "명시적 거절 ↔ 발급 없음 = 정상" 으로 접어 **결과 불명이 정상이 된다.**
 * 실측으로 재현했다 — 종료코드 0, MATCHED_REJECTED.
 *
 * 코드 목록을 여기 베끼지 않는다. 서버가 이미 자기 계약으로 표시해 준다 —
 * *"재시도를 권하는 문구는 기다리면 상태가 바뀔 수 있는 코드에만 쓴다"*
 * (CouponIssueV2ErrorCode). 그 표시가 `Retry-After` 헤더다. 목록을 베끼면 서버가
 * 코드를 하나 더 만들 때 여기만 낡는다.
 *
 * ⚠️ k6 는 헤더 이름을 **정규 표기**로 준다. `headers['Retry-After']` 는 값이 있고
 *    `headers['retry-after']` 는 `undefined` 다(실측). 소문자로 찾으면 조용히 못 찾는다.
 */
export function classify(res) {
  // 연결이 아예 안 된 실패는 status 0 이다. **"안 됐다" 가 아니다** — 타임아웃은
  // 서버가 이미 커밋했을 수 있다. 가르는 것은 대조가 DB 를 보고 할 일이다.
  if (res.status === 0) {
    return { kind: UNKNOWN, reason: String(Number(res.error_code || 0)) };
  }
  if (res.status === 201) {
    return { kind: OK, reason: issuanceIdOf(res) };
  }
  // 5xx 는 명시적 거절이 아니다. 서버가 커밋하고 응답에서 터졌을 수 있다.
  if (res.status >= 500) {
    return { kind: UNKNOWN, reason: `HTTP-${res.status}` };
  }
  const code = errorCodeOf(res);
  if (res.headers && res.headers['Retry-After'] !== undefined) {
    return { kind: DEFERRED, reason: code };
  }
  return { kind: REJECTED, reason: code };
}

// 201 응답의 예약번호. 없으면 빈 문자열이다 — **없다는 사실도 기록한다.**
// 201 인데 id 가 없으면 응답 계약이 깨진 것이고, 그것을 조용히 빼면 대조가
// "그 건은 애초에 없었다" 로 읽는다.
export function issuanceIdOf(res) {
  try {
    const body = res.json();
    const id = body && body.data && body.data.issuanceId;
    return id === undefined || id === null ? '' : String(id);
  } catch (e) {
    return '';
  }
}

export function errorCodeOf(res) {
  try {
    const body = res.json();
    return (body && body.error && body.error.code) || `HTTP-${res.status}`;
  } catch (e) {
    return `HTTP-${res.status}`;
  }
}
