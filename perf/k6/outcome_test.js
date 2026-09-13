// 응답 판정을 태운다. **서버도 환경변수도 필요 없다** — 가짜 응답만 넣는다.
//
//   docker run --rm --network none -v "$PWD/perf/k6:/k6:ro" -w /k6 \
//     grafana/k6:latest run outcome_test.js
//
// ⚠️ **예외를 던지는 것으로는 시험이 안 된다.** k6 는 기본 함수가 throw 해도
//    종료코드 0 으로 끝난다(실측). 임계값이 깨져야 99 가 나온다 — 그래서 실패를
//    카운터로 세고 `count==0` 임계값을 건다. 이 구조를 바꾸면 항상 통과하는
//    가짜 게이트가 된다.
import { Counter } from 'k6/metrics';
import { classify, OK, REJECTED, DEFERRED, UNKNOWN } from './outcome.js';

const failures = new Counter('outcome_failures');

export const options = {
  scenarios: { t: { executor: 'per-vu-iterations', vus: 1, iterations: 1 } },
  thresholds: { outcome_failures: ['count==0'] },
};

// k6 의 http 응답 흉내. json() 이 본문을 파싱한다.
function res(status, body, headers) {
  return {
    status: status,
    error_code: 0,
    headers: headers || {},
    json: function () {
      if (body === undefined) {
        throw new Error('본문 없음');
      }
      return body;
    },
  };
}

function check(label, got, wantKind, wantReason) {
  if (got.kind !== wantKind || (wantReason !== undefined && got.reason !== wantReason)) {
    failures.add(1);
    console.error(
      `${label}: kind=${got.kind} reason=${got.reason} — 기대 ${wantKind} ${wantReason}`);
  }
}

export default function () {
  const issued = { data: { issuanceId: 777 } };
  const soldOut = { error: { code: 'COUPON-306' } };
  const pending = { error: { code: 'COUPON-320' } };

  check('성공', classify(res(201, issued)), OK, '777');
  check('성공인데 예약번호 없음', classify(res(201, { data: {} })), OK, '');
  check('성공인데 본문 깨짐', classify(res(201, undefined)), OK, '');

  check('매진 거절', classify(res(409, soldOut)), REJECTED, 'COUPON-306');
  check('이미 발급', classify(res(409, { error: { code: 'COUPON-305' } })),
        REJECTED, 'COUPON-305');
  check('등급 거절', classify(res(403, { error: { code: 'COUPON-304' } })),
        REJECTED, 'COUPON-304');

  // 여기가 이 시험의 본체다. **같은 409 인데 갈린다.**
  check('처리 중 (Retry-After 있음)',
        classify(res(409, pending, { 'Retry-After': '3' })), DEFERRED, 'COUPON-320');
  check('매진에 Retry-After 가 붙으면 그것도 판정이 아니다',
        classify(res(409, soldOut, { 'Retry-After': '1' })), DEFERRED, 'COUPON-306');

  // k6 는 정규 표기로 준다. 소문자만 있는 응답을 판정 보류로 읽으면 안 된다 —
  // 그런 응답은 우리 서버가 내지 않는다.
  check('소문자 retry-after 는 안 본다',
        classify(res(409, pending, { 'retry-after': '3' })), REJECTED, 'COUPON-320');

  check('5xx 는 결과 불명', classify(res(500, { error: { code: 'COUPON-307' } })),
        UNKNOWN, 'HTTP-500');
  check('503 도 결과 불명', classify(res(503, undefined)), UNKNOWN, 'HTTP-503');
  check('연결 실패는 결과 불명', classify({ status: 0, error_code: 1050, headers: {} }),
        UNKNOWN, '1050');
}
