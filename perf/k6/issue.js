// ③ 발급 부하 — 워밍업 한 구간 + 측정 한 구간.
//
// 계단(steps)은 이 스크립트가 아니라 run/run-round.sh 가 만든다. 단계마다 회차가
// 달라야 하기 때문이다 — 한 회차로 계단을 올리면 첫 단계에서 재고가 다 나가고
// 이후는 전부 매진 거절이라 발급 경로를 안 탄다(docs/12 §10.2).
//
// 워밍업을 계단 앞에 두는 근거는 실측이다. 300/s x 25s 를 앞에 두니 같은 구간에서
// med 24ms -> 4ms, p95 861ms -> 12ms 로 떨어졌다. 버퍼풀과 JIT 가 지연 꼬리의 대부분이었다.
import http from 'k6/http';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { classify, OK, REJECTED, DEFERRED, UNKNOWN } from './outcome.js';

const BASE_URL = __ENV.BASE_URL;
const TIMEOUT = __ENV.HTTP_TIMEOUT || '60s';

// ④ 독립 요청 기록. **요청을 보내기 전에** 접수 키·대상을 남기고, 응답을 받은 뒤
//    결과를 같은 키에 잇는다.
//
// **성공만 모으면 안 된다.** 응답이 유실된 건은 성공 목록에서 통째로 빠지는데,
// 그 건은 접수됐을 수도 안 됐을 수도 있다 — **어느 쪽인지 모른다는 것이 사실**이고
// 그 사실이 기록에 남아야 한다. 안 남기면 그 건이 "정상" 으로 보이거나(안 세면)
// "유실" 오탐이 된다(세면).
//
// 그리고 **양쪽에서 동시에 빠진 건**은 성공 목록으로도 DB 대조로도 못 잡는다.
// 요청을 보냈다는 기록이 **요청보다 먼저** 있어야만 보인다.
//
// 기본은 꺼짐이다. **속도 때문이 아니다** — 1,000~6,667/s 에서 끔/켬의 달성 도착률
// 차이가 같은 조건의 흔들림보다 작았고, 1.3만 줄/초에서 한 줄도 안 샜다(실측:
// docs/measurements/record-overhead.sh). 회차당 4만 줄이 쌓이는 것이 이유고,
// 대조할 회차에서만 켠다.
const RECORD = String(__ENV.RECORD_REQUESTS || 'false') === 'true';

// 결과 불명이면 **같은 접수 키로** 한 번 더 보낸다. 명세 F-U-03 이 요구하는 복구가
// 그 경로이고(*"같은 키로 결과를 확인"*), 그러지 않으면 게이트의 REPLAY_DONE·
// REPLAY_PENDING 이 회차 내내 한 번도 안 생긴다 — 대조의 "재전송은 한 신청" 판정도
// 픽스처 밖에서는 만난 적이 없게 된다.
//
// 기본은 꺼짐이다. 켠 회차와 끈 회차는 **서버가 받는 요청 수가 다르므로**,
// 나란히 비교할 회차끼리는 같은 설정이어야 한다.
const RETRY_UNKNOWN = String(__ENV.RETRY_UNKNOWN || 'false') === 'true';
// ⚠️ Retry-After 의 초를 그대로 따르지 않는다. 3초를 자면 재전송마다 VU 가 묶여
//    도착률을 맞추려고 VU 가 몇 배로 늘고, 그러면 재려던 것이 바뀐다. 짧게 쉰다 —
//    그래서 REPLAY_PENDING 에 곧바로 다시 물어 또 PENDING 을 받을 수 있고,
//    그것도 사실이라 그대로 기록한다.
const RETRY_DELAY_MS = Number(__ENV.RETRY_DELAY_MS || 200);

const WARMUP_ROUND = __ENV.WARMUP_ROUND_ID;
const TARGET_ROUND = __ENV.TARGET_ROUND_ID;
const WARMUP_RATE = Number(__ENV.WARMUP_RATE || 300);
const WARMUP_SECONDS = Number(__ENV.WARMUP_SECONDS || 25);
const TARGET_RATE = Number(__ENV.TARGET_RATE);
const TARGET_SECONDS = Number(__ENV.TARGET_SECONDS);
// ⚠️ 회원 id 는 members 에 실제로 있는 범위여야 한다. issuances 에 members FK 가 있어서
//    없는 memberId 를 주면 전부 500 이다 — 그것도 응답 코드만 보면 서버 결함처럼 보인다.
//    워밍업과 측정은 서로 다른 회차를 쓰므로(1인 1매는 회차 단위) 둘 다 1 부터 쓴다.
const WARMUP_MEMBER_BASE = Number(__ENV.WARMUP_MEMBER_BASE || 1);
const MEMBER_BASE = Number(__ENV.MEMBER_BASE || 1);

// ⚠️ 등급 헤더는 회차의 eligible_grades_mask 와 맞아야 한다. 여기에 값을 하드코딩하면
//    마스크가 바뀐 회차에서 등급 거절로 전량 실패하고, 결과에는 "거절 N건"으로만 보여
//    원인을 못 찾는다. run-round.sh 가 회차의 마스크에서 뽑아 넘긴다 —
//    기본값을 두지 않는 것이 이 규약의 전부다.
const MEMBER_GRADE = __ENV.MEMBER_GRADE;
const WARMUP_MEMBER_GRADE = __ENV.WARMUP_MEMBER_GRADE;
if (!MEMBER_GRADE || !WARMUP_MEMBER_GRADE) {
  throw new Error('MEMBER_GRADE · WARMUP_MEMBER_GRADE 가 필요하다. '
    + '회차의 eligible_grades_mask 에서 뽑은 등급이어야 한다 (run-round.sh 가 넘긴다).');
}

// 성공과 거절을 반드시 나눈다. 섞은 단일 p99 는 매진 거절 1만 건이 분포를 끌어내려
// 실제보다 좋아 보인다.
const successes = new Counter('issue_successes');
const rejections = new Counter('issue_rejections');
const errors = new Counter('issue_errors');
// 서버가 **아직 안 정했다**고 말한 4xx. `Retry-After` 가 붙은 응답이다.
// 거절과 같은 칸에 넣으면 안 된다 — 거절은 판정이고 이쪽은 판정이 없는 상태다.
const deferred = new Counter('issue_deferred');
// 같은 키로 다시 보낸 횟수. **이터레이션이 아니다** — 도착률에 안 들어간다.
const retries = new Counter('issue_retries');
// ⚠️ 내장 http_reqs 는 워밍업 시나리오까지 합산한다. 그 rate 는 "워밍업 + 대기 + 측정"
//    전체 실행 시간으로 나눈 값이라 측정 구간의 달성 도착률이 아니다. 실측으로
//    설정 800/s x 5s 회차에서 http_reqs.rate 가 185/s 로 나왔다 —
//    (워밍업 1000건 + 측정 4001건) / 27초였다. 측정 구간만 따로 센다.
const measureAttempts = new Counter('issue_attempts');
// ⚠️ 측정 구간의 실제 시각. Prometheus range query 를 이 창으로 잘라야 한다 —
//    k6 프로세스 전체를 창으로 쓰면 워밍업 25초 + 대기 12초 + gracefulStop 이 전부
//    들어가 CPU·scrape 수치가 측정 구간 것이 아니게 된다. Trend 의 min·max 가
//    첫 이터레이션과 마지막 이터레이션의 시작 시각이라 그대로 창이 된다.
const measureClock = new Trend('measure_clock_ms');
// 응답이 아예 없는 실패(status 0). 서버 지연으로 세면 안 된다.
// ⚠️ status 0 을 전부 "연결 실패" 로 뭉치면 안 된다 — 60초 요청 타임아웃도 여기 들어온다.
//    톰캣 수용 상한·임시 포트 고갈의 근거로 쓰려면 연결 실패만 따로 세야 하고,
//    타임아웃은 오히려 "서버가 받긴 했는데 못 끝냈다" 라 정반대 진단이다.
//    k6 error_code 대역으로 가른다 — 1100번대 DNS · 1200번대 연결 · 1050 요청 타임아웃.
const connectFailures = new Counter('issue_connect_failures');
const timeouts = new Counter('issue_timeouts');
const otherTransportErrors = new Counter('issue_transport_errors');

const successDuration = new Trend('issue_success_duration', true);
const soldOutDuration = new Trend('issue_rejected_sold_out_duration', true);
const otherRejectDuration = new Trend('issue_rejected_other_duration', true);
const warmupDuration = new Trend('warmup_duration', true);

const preAllocated = Math.max(200, Math.ceil(Math.max(WARMUP_RATE, TARGET_RATE) * 1.5));

export const options = {
  discardResponseBodies: false,
  // 기본 요약에는 p(99) 가 없다. 목표가 p99 인데 요약에 없으면 결과 JSON 에서
  // 그 값을 다시 만들 방법이 없다.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    warmup: {
      executor: 'constant-arrival-rate',
      exec: 'warmup',
      rate: WARMUP_RATE,
      timeUnit: '1s',
      duration: `${WARMUP_SECONDS}s`,
      preAllocatedVUs: preAllocated,
      maxVUs: preAllocated * 4,
      gracefulStop: '10s',
      tags: { phase: 'warmup' },
    },
    measure: {
      executor: 'constant-arrival-rate',
      exec: 'measure',
      // 워밍업이 끝나고 그 gracefulStop 까지 지난 뒤에 시작한다. 겹치면 워밍업 트래픽이
      // 측정 구간의 도착률에 섞인다.
      startTime: `${WARMUP_SECONDS + 12}s`,
      rate: TARGET_RATE,
      timeUnit: '1s',
      duration: `${TARGET_SECONDS}s`,
      preAllocatedVUs: preAllocated,
      maxVUs: preAllocated * 4,
      gracefulStop: '60s',
      tags: { phase: 'measure' },
    },
  },
};

// (회차, 회원) -> UUID v4 형식 문자열. 결정적이라 같은 회차를 다시 돌리면 같은 키가 나온다.
//
// **32비트로는 모자란다.** 예전에는 FNV-1a 상태 하나(32비트)에서 32개 니블을 전부
// 뽑았다 — xorshift 확장은 엔트로피를 안 늘리므로 서로 다른 키가 최대 2^32 개였고,
// 회차당 2만 건이면 생일 한계로 충돌 확률이 4.55% 다.
//
// 충돌이 나면 조용히 두 군데가 망가진다. **서버**는 서로 다른 회원의 요청을 같은
// 멱등 요청으로 보고 앞 사람의 응답을 되돌려 준다 — 뒤 사람은 쿠폰을 못 받았는데
// 201 을 본다. **대조**는 두 요청을 한 신청으로 묶어 "대상이 2가지" 라고 낸다.
// 둘 다 서버 결함처럼 보이지만 실은 부하 도구의 결함이다.
//
// 실측: 32비트로는 회차 400개 x 2만 건(키 800만 개)에서 충돌 0쌍이었지만 한 회차에
// 회원 200만 명을 넣으면 32쌍이 났다. **지금 규모에서 안 났을 뿐 천장은 실재한다.**
// 서로 다른 오프셋으로 FNV-1a 를 네 번 돌려 128비트를 만든다.
function uuidV4From(roundId, memberId) {
  const seed = `${roundId}:${memberId}`;
  // FNV-1a 의 표준 오프셋과, 그것에서 갈라 놓은 값 셋. 같은 곱수를 쓰되 시작점이
  // 다르면 네 갈래가 서로 독립적으로 흩어진다.
  const offsets = [2166361, 2166136261, 84696351, 3402289871];
  const hex = [];
  for (let lane = 0; lane < 4; lane++) {
    let h = offsets[lane] >>> 0;
    for (let i = 0; i < seed.length; i++) {
      h ^= seed.charCodeAt(i);
      h = Math.imul(h, 16777619) >>> 0;
    }
    // ⚠️ FNV 상태를 **그대로 내보내면 안 된다.** 곱셈이 낮은 자리에서 높은 자리로만
    //    번지므로, 회원 번호만 1씩 다른 입력에서는 상당수 니블이 그대로 남는다.
    //    실제로 확산 없이 찍어 봤더니 `c945f480-…` `cc45f939-…` `cb45f7a6-…` 처럼
    //    자리마다 같은 글자가 박혔다. fmix32 로 한 번 흩고 낸다.
    h ^= h >>> 16; h = Math.imul(h, 0x85ebca6b) >>> 0;
    h ^= h >>> 13; h = Math.imul(h, 0xc2b2ae35) >>> 0;
    h ^= h >>> 16;
    // 한 갈래가 니블 8개(32비트)를 낸다. 갈래마다 독립이라 합쳐서 128비트다.
    for (let i = 28; i >= 0; i -= 4) {
      hex.push(((h >>> i) & 0xf).toString(16));
    }
  }
  hex[12] = '4';                                        // 버전 4
  hex[16] = ((parseInt(hex[16], 16) & 0x3) | 0x8).toString(16);  // variant 10xx
  const s = hex.join('');
  return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20)}`;
}

// 한 줄 = 한 사건. 하네스가 `--log-format=raw --console-output=<파일>` 로 돌리므로
// 이 줄은 k6 진행 로그와 섞이지 않고 그 파일에만 들어간다. 실측한 것 둘.
//
//   · `--log-format=raw` 가 없으면 k6 가 logfmt 으로 감싸 `msg="CY960\tREQ\t..."` 로
//     나가고 **탭이 이스케이프된다.** 그러면 읽는 쪽이 logfmt 을 풀고 다시
//     언이스케이프해야 한다.
//   · `--console-output` 이 담는 것은 **스크립트의 console.* 뿐**이다. k6 자신의
//     경고(예: `Request Failed`)는 stderr 로 갔다.
//
// 그래도 접두사를 뗄 수 없다. 같은 파일에 다른 `console.*` 한 줄만 들어와도 기록이
// 오염되는데, **그 오염은 대조 결과가 틀린 뒤에야 보인다.**
//
// 탭 구분이다. 값에 탭이 없다는 것이 전제이고, 접수 키는 UUID·대상은 숫자다.
function record(kind, fields) {
  if (!RECORD) {
    return;
  }
  console.log(`CY960\t${kind}\t${fields.join('\t')}`);
}

function issue(roundId, memberId, grade) {
  return http.post(
    `${BASE_URL}/api/v1/coupons/${roundId}/issue`,
    null,
    {
      headers: {
        'X-Member-Id': String(memberId),
        'X-Member-Grade': grade,
        // ⚠️ 서버가 UUID v4 형식을 강제한다. 형식이 아니면 발급 경로를 타기도 전에
        //    COUPON-300 으로 전량 거절되고, 그 회차는 "거절 6001건"으로 보인다(실측).
        //    요청마다 달라야 하지만 재현 가능해야 하므로 (회차, 회원)에서 결정적으로 만든다.
        'Idempotency-Key': uuidV4From(roundId, memberId),
      },
      timeout: TIMEOUT,
      tags: { name: 'issue' },
    },
  );
}

// 기록이 켜진 회차라는 표식. **한 줄이지만 이게 없으면 대조가 위험해진다.**
//
// `--console-output` 은 기록을 꺼도 파일을 만든다. 그 빈 파일을 "요청 0건" 으로 읽으면
// 대조가 **회차의 발급 전부를 고아로** 보고한다 — 아무 문제 없는 회차에서 만 건짜리
// 거짓 결함이 나온다. 그래서 대조는 이 줄이 있을 때만 판정한다.
//
// 회차 번호를 함께 싣는 이유는 따로 있다. `--console-output` 은 **덮어쓰지 않고 이어
// 쓴다**(실측). 같은 디렉터리에 두 번 쏘면 앞 회차의 기록이 그대로 남아 있고, 그 키들은
// 이번 회차 DB 에 없으니 전부 미해결로 보인다. 대조가 회차로 걸러 낸다.
export function setup() {
  record('RUN', [TARGET_ROUND]);
  return {};
}

export function warmup() {
  const memberId = WARMUP_MEMBER_BASE + exec.scenario.iterationInTest;
  const res = issue(WARMUP_ROUND, memberId, WARMUP_MEMBER_GRADE);
  warmupDuration.add(res.timings.duration);
}

export function measure() {
  measureAttempts.add(1);
  measureClock.add(Date.now());
  // 회차 단위 1인 1매다. 매 요청 서로 다른 회원이어야 하고, 그래서 요청 수만큼
  // 회원이 필요하다. 겹치면 ALREADY_ISSUED 가 나고 발급 경로가 아니라 멱등 경로를 잰다.
  const memberId = MEMBER_BASE + exec.scenario.iterationInTest;
  const key = uuidV4From(TARGET_ROUND, memberId);

  // **보내기 전에** 남긴다. 여기서 프로세스가 죽어도 REQ 는 남고, 짝이 되는 RES 가
  // 없는 것이 곧 "결과 불명" 이다 — 대조가 그것을 미해결로 다룬다.
  record('REQ', [key, TARGET_ROUND, memberId]);

  let res = issue(TARGET_ROUND, memberId, MEMBER_GRADE);
  let outcome = classify(res);
  record(outcome.kind === DEFERRED ? 'UNKNOWN' : outcome.kind, [key, outcome.reason]);

  // 결과 불명이면 **같은 키로** 한 번만 더 보낸다. 무한 재시도는 안 한다 — 계속
  // 불명이면 그것이 사실이고, 대조가 미해결로 남기는 것이 맞다.
  if (RETRY_UNKNOWN
      && (outcome.kind === UNKNOWN || outcome.kind === DEFERRED)) {
    retries.add(1);
    sleep(RETRY_DELAY_MS / 1000);
    // 보내기 전에 남긴다. 첫 요청과 같은 규칙이다.
    record('REQ', [key, TARGET_ROUND, memberId]);
    res = issue(TARGET_ROUND, memberId, MEMBER_GRADE);
    outcome = classify(res);
    record(outcome.kind === DEFERRED ? 'UNKNOWN' : outcome.kind, [key, outcome.reason]);
  }

  // 지표는 **마지막 응답 하나**만 센다. 한 이터레이션이 여러 칸을 올리면
  // 성공+거절+보류+5xx 합이 시도 수와 안 맞아 요약표의 산수가 깨진다.
  // 재전송이 있었다는 사실은 issue_retries 가 따로 말한다.
  const { kind, reason } = outcome;
  if (kind === OK) {
    successes.add(1);
    successDuration.add(res.timings.duration);
    return;
  }
  // ⚠️ 연결이 아예 안 된 실패는 status 0 이고 duration 이 0 이다. 이것을 응답 지연으로
  //    세면 분포가 통째로 거짓이 된다. 톰캣 수용 상한(max-connections + accept-count)을
  //    넘기면 실제로 이쪽으로 찍힌다.
  if (res.status === 0) {
    const ec = Number(res.error_code || 0);
    const tag = { error_code: String(ec) };
    if (ec === 1050) {
      timeouts.add(1, tag);                    // 요청 타임아웃
    } else if ((ec >= 1200 && ec < 1300) || (ec >= 1100 && ec < 1200)) {
      connectFailures.add(1, tag);             // 연결 거부·리셋·DNS
    } else {
      otherTransportErrors.add(1, tag);        // TLS·HTTP2·그 밖
    }
    return;
  }
  if (res.status >= 500) {
    errors.add(1, { code: reason });
    return;
  }
  if (kind === DEFERRED) {
    // 서버가 **아직 안 정했다.** 거절과 같은 칸에 넣으면 판정이 아닌 것이 판정이 된다.
    deferred.add(1, { code: reason });
    return;
  }
  rejections.add(1, { code: reason });
  if (reason === 'COUPON-306') {
    soldOutDuration.add(res.timings.duration);
  } else {
    otherRejectDuration.add(res.timings.duration);
  }
}

export function handleSummary(data) {
  // 달성 도착률은 "측정 구간에 실제로 쏜 요청 / 측정 구간 길이" 다.
  // 못 쏜 것은 시간에 늘어지지 않고 dropped_iterations 로 빠지므로 이 나눗셈이 정확하다.
  const attempts = (data.metrics.issue_attempts
    && data.metrics.issue_attempts.values.count) || 0;
  const clock = data.metrics.measure_clock_ms && data.metrics.measure_clock_ms.values;
  const retryCount = (data.metrics.issue_retries
    && data.metrics.issue_retries.values.count) || 0;
  data.perf = {
    measure_attempts: attempts,
    // 같은 키로 다시 보낸 횟수. 대조가 `기록 줄 수 == 시도 + 재전송` 을 본다.
    measure_retries: retryCount,
    // Prometheus 질의를 자를 창. 없으면 호출부가 <측정 실패> 로 다뤄야 한다.
    measure_window_start_epoch: clock ? Math.floor(clock.min / 1000) : null,
    measure_window_end_epoch: clock ? Math.ceil(clock.max / 1000) : null,
    target_seconds: TARGET_SECONDS,
    configured_rate_per_sec: TARGET_RATE,
    achieved_arrival_rps: TARGET_SECONDS > 0 ? attempts / TARGET_SECONDS : null,
    http_reqs_rate_note:
      'metrics.http_reqs.rate 는 워밍업 시나리오까지 합산한 전체 실행 평균이다. '
      + '측정 구간의 달성 도착률로 쓰지 말 것 — achieved_arrival_rps 를 쓴다.',
  };
  const out = {};
  const path = __ENV.OUT_JSON;
  if (path) out[path] = JSON.stringify(data, null, 2);
  out.stdout = textLine(data);
  return out;
}

function textLine(data) {
  const m = data.metrics;
  const g = (name, field) => (m[name] && m[name].values && m[name].values[field] !== undefined
    ? m[name].values[field] : null);
  // 한 번도 안 오른 Counter 는 요약에 아예 없다. 그건 0 이지 측정 실패가 아니다.
  // Trend 는 표본이 없으면 값을 만들 수 없으므로 측정 실패다. 둘을 섞으면
  // "성공 0건"과 "성공 지연을 못 쟀다"가 같은 칸에 찍힌다.
  const c = (name) => (g(name, 'count') === null ? 0 : g(name, 'count'));
  const fmt = (v) => (v === null ? '측정 실패' : Math.round(v * 100) / 100);
  return [
    '',
    `대상 회차 ${TARGET_ROUND} · 설정 도착률 ${TARGET_RATE}/s x ${TARGET_SECONDS}s`,
    `  달성 도착률(실측)  ${fmt(data.perf.achieved_arrival_rps)}/s      <- 측정 구간만. 설정값과 다르다`,
    `  (참고) http_reqs.rate ${fmt(g('http_reqs', 'rate'))}/s   <- 워밍업 포함 전체 평균. 달성치가 아니다`,
    `  성공             ${fmt(c('issue_successes'))}`,
    `  거절             ${fmt(c('issue_rejections'))}`,
    `  5xx              ${fmt(c('issue_errors'))}`,
    `  연결 실패        ${fmt(c('issue_connect_failures'))}   <- 응답이 아니다. 수용 상한·임시 포트를 본다`,
    `  타임아웃         ${fmt(c('issue_timeouts'))}   <- 받긴 했는데 못 끝냈다. 연결 실패와 진단이 반대다`,
    `  기타 전송 오류   ${fmt(c('issue_transport_errors'))}`,
    `  못 쏜 것         ${fmt(c('dropped_iterations'))}`,
    `  성공 med/p95/p99 ${fmt(g('issue_success_duration', 'med'))} / ${fmt(g('issue_success_duration', 'p(95)'))} / ${fmt(g('issue_success_duration', 'p(99)'))} ms`,
    `  매진 med/p99     ${fmt(g('issue_rejected_sold_out_duration', 'med'))} / ${fmt(g('issue_rejected_sold_out_duration', 'p(99)'))} ms`,
    '',
  ].join('\n');
}
