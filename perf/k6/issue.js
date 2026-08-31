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
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL;
const TIMEOUT = __ENV.HTTP_TIMEOUT || '60s';

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
function uuidV4From(roundId, memberId) {
  let h = 2166136261 >>> 0;                 // FNV-1a
  const seed = `${roundId}:${memberId}`;
  for (let i = 0; i < seed.length; i++) {
    h ^= seed.charCodeAt(i);
    h = Math.imul(h, 16777619) >>> 0;
  }
  const hex = [];
  let x = h;
  for (let i = 0; i < 32; i++) {
    x ^= x << 13; x >>>= 0;
    x ^= x >>> 17;
    x ^= x << 5;  x >>>= 0;
    hex.push((x & 0xf).toString(16));
  }
  hex[12] = '4';                                        // 버전 4
  hex[16] = ((parseInt(hex[16], 16) & 0x3) | 0x8).toString(16);  // variant 10xx
  const s = hex.join('');
  return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20)}`;
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
  const res = issue(TARGET_ROUND, memberId, MEMBER_GRADE);

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

  if (res.status === 201) {
    successes.add(1);
    successDuration.add(res.timings.duration);
    return;
  }

  const code = errorCodeOf(res);
  if (res.status >= 500) {
    errors.add(1, { code: code });
    return;
  }
  rejections.add(1, { code: code });
  if (code === 'COUPON-306') {
    soldOutDuration.add(res.timings.duration);
  } else {
    otherRejectDuration.add(res.timings.duration);
  }
}

function errorCodeOf(res) {
  try {
    const body = res.json();
    return (body && body.error && body.error.code) || `HTTP-${res.status}`;
  } catch (e) {
    return `HTTP-${res.status}`;
  }
}

export function handleSummary(data) {
  // 달성 도착률은 "측정 구간에 실제로 쏜 요청 / 측정 구간 길이" 다.
  // 못 쏜 것은 시간에 늘어지지 않고 dropped_iterations 로 빠지므로 이 나눗셈이 정확하다.
  const attempts = (data.metrics.issue_attempts
    && data.metrics.issue_attempts.values.count) || 0;
  const clock = data.metrics.measure_clock_ms && data.metrics.measure_clock_ms.values;
  data.perf = {
    measure_attempts: attempts,
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
