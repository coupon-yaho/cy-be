package com.kafkick.infra.redis.coupon.v2;

/**
 * v2 발급 Lua 5종의 반환 코드. <b>숫자를 호출부에 흩뿌리지 않는다</b> — 같은 {@code -2} 가
 * 스크립트마다 다른 뜻이라(완료는 "남의 선점", 배치 복원은 "상한 초과") 리터럴로 다루면
 * 응답 매핑(S5)과 경보가 조용히 어긋난다.
 *
 * <p>값의 출처는 {@code docs/14-v2-phase0/02·03·06} 이다. 코드는 문서의 표 그대로다.
 */
public final class IssuanceScriptCodes {

    private IssuanceScriptCodes() {
    }

    /** 선점 — 첫 원소가 코드, 성공일 때 둘째 원소가 잔여 재고다. */
    public static final class Claim {

        public static final long OK = 0;
        public static final long CLOSED = -1;
        public static final long NOT_OPEN = -2;
        public static final long GRADE_NOT_ALLOWED = -3;
        public static final long DUP_PER_MEMBER = -4;
        public static final long SOLD_OUT = -5;
        public static final long REPLAY_DONE = -6;
        public static final long REPLAY_PENDING = -7;
        /** 값 형식 파손. 정상 운영에서 0 이어야 한다. */
        public static final long CORRUPT_VALUE = -8;
        /** 게이트 미준비(재구성 중) — meta 가 <b>부분 상태</b>이거나 숫자가 아닌 경우를 포함한다. */
        public static final long NOT_READY = -9;
        /**
         * 인자 이상 — 인자 개수 부족 · 빈 {@code memberId} · 빈 멱등키 · 빈 토큰 ·
         * {@code '|'} 가 든 토큰 · 음수·소수·비숫자·32비트 초과 등급 비트. 조건은 12 문서의 표와 같다.
         * 이걸 그대로 쓰면 <b>자기 파서가 파손이라 부를 값을 자기가 만든다.</b> 버그이므로 경보.
         */
        public static final long BAD_ARGUMENT = -10;
        /**
         * {@code stock} 부재 또는 세 카운터의 비정수·자료형 오류. {@code issued_ever} 와
         * {@code issued_revision} 부재는 초기 상태로 허용하며 성공한 {@code INCR} 이 생성한다.
         * <b>매진과 합치지 않는다.</b> 합쳐 두면 재고가 남아 있는데도 전량 종단 거절되고,
         * 재구성 창의 정상적인 {@code -9} 에 사고가 묻힌다. {@code -9} 는 "기다리면 풀린다",
         * 이건 "운영이 개입해야 한다" 다.
         *
         * <p>이름이 {@code STOCK_MISSING} 이 아닌 이유 — <b>선점은 세 카운터를 다 본다.</b>
         * 경보를 받은 사람이 {@code GET stock} 만 확인하고 닫으면 원인이
         * {@code issued_ever}·{@code issued_revision} 일 때 장애가 그대로 이어진다.
         * {@code Restore} 는 {@code stock} 하나만 보므로 거기서는 이름이 그대로다 —
         * <b>이름이 점검 범위다.</b>
         */
        public static final long COUNTER_UNREADABLE = -11;

        private Claim() {
        }
    }

    /** 완료 CAS. */
    public static final class Complete {

        public static final long PROMOTED = 1;
        /** 이미 {@code D} — 재시도끼리 겹친 것이라 정상이다. */
        public static final long ALREADY_DONE = 0;
        /** field 가 사라졌다. 보상과 겹쳤다. */
        public static final long CLAIM_GONE = -1;
        public static final long FOREIGN_CLAIM = -2;
        /** issued 값 형식 또는 issued_revision이 파손되어 완료 승격을 거절했다. */
        public static final long CORRUPT_VALUE = -3;
        /**
         * 인자 이상 — 쓸 수 없는 요청토큰(빈 값 · {@code '|'} 포함).
         * 선점의 {@code -10} 과 같은 뜻이다. <b>완료시각은 인자가 아니다</b> —
         * 승격은 선점시각을 그대로 둔다.
         */
        public static final long BAD_ARGUMENT = -10;

        private Complete() {
        }
    }

    /** 보상 CAS. */
    public static final class Compensate {

        public static final long REVERTED = 1;
        /** 이미 없거나 내 선점이 아니다 — 아무것도 하지 않았다. 정상이다. */
        public static final long NOT_MINE = 0;
        /** 이미 {@code D} 다. 보상 금지 — 경보. */
        public static final long ALREADY_DONE = -1;
        public static final long CORRUPT_VALUE = -3;
        /**
         * {@code stock}·{@code issued_ever}·{@code issued_revision} 을 읽을 수 없다 —
         * 비숫자 · <b>비정수</b> · 자료형 오류.
         * <b>되돌리기 전에</b> 본다. 통과시키면 {@code HDEL}·{@code INCR} 만 적용된 채
         * {@code DECR} 이 터진다 — Lua 는 원자적이어도 이미 적용된 쓰기를 되돌리지 않는다.
         */
        public static final long COUNTER_UNREADABLE = -11;
        /**
         * 인자 이상 — <b>인자 개수 부족</b> · 빈 {@code memberId} · 쓸 수 없는 요청토큰(빈 값 ·
         * {@code '|'} 포함). 조건은 12 문서의 표와 같다.
         * <b>저장된 값은 이 인자들과 같아질 수 없다.</b> 가드가 없으면
         * 항상 {@code 0}(내 것이 아님 = 정상)이 나가고, 선점 때 깎인 재고가 조용히 잠긴다.
         */
        public static final long BAD_ARGUMENT = -10;

        private Compensate() {
        }
    }

    /**
     * 파손 값 회수. <b>되돌리는 범위를 호출부가 정한다</b> — 파손된 값에는 원래 상태가
     * 남아 있지 않아 스크립트 단독으로는 `P` 였는지 `D` 였는지 알 수 없다. 호출부(S8)가
     * 게이트가 닫힌 상태에서 DB 를 조회해 그 결과를 플래그로 넘긴다.
     */
    public static final class Reclaim {

        /** 지우고 {@code stock}·{@code issued_ever} 까지 되돌렸다 — DB 에 발급이 없었다. */
        public static final long RECLAIMED_AND_RESTORED = 1;
        /** field 만 지웠다 — DB 에 발급이 있어 재고를 되살리면 초과 발급이다. */
        public static final long RECLAIMED_ONLY = 2;
        /** field 가 이미 없다. 정상이다. */
        public static final long NOTHING = 0;
        /** 파손이 아니다 — 살아 있는 선점이라 건드리지 않았다. */
        public static final long NOT_CORRUPT = -1;
        /** {@code stock}·{@code issued_ever}·{@code issued_revision} 을 읽을 수 없다. */
        public static final long COUNTER_UNREADABLE = -11;
        /**
         * 상한 초과 — {@code stock + 1 > total}. <b>파손 값에는 재고를 깎았다는 증거가 없다.</b>
         * 선점은 인자 가드 때문에 파손 값을 만들 수 없으므로, 회수 대상의 출처는 재구성
         * 재작성·수동 조작·구버전이고 "field 하나 = DECR 한 번" 이 보장되지 않는다.
         */
        public static final long OVER_CAP = -2;
        /**
         * 인자 이상 — <b>인자 개수 부족</b> · 빈 {@code memberId} · 복원 여부 플래그가
         * {@code '0'}/{@code '1'} 이 아님 · 총재고가 canonical 정수가 아님. 호출부 버그다.
         * 조건은 12 문서의 표와 같다.
         */
        public static final long BAD_ARGUMENT = -10;

        private Reclaim() {
        }
    }

    /** 만료 배치의 재고 복원. */
    public static final class Restore {

        public static final long RESTORED = 1;
        /** 게이트 미준비(재구성 중). 건너뛰고 건수를 카운터로 남긴다. */
        public static final long NOT_READY = -1;
        /** 상한 초과. 그 회차 만료 처리를 멈추고 경보한다. */
        public static final long OVER_CAP = -2;
        /** 인자 이상. 버그다. */
        public static final long BAD_ARGUMENT = -3;
        /** {@code stock} 을 읽을 수 없다. 선점의 {@code -11} 과 같은 뜻이다. */
        public static final long STOCK_MISSING = -11;

        private Restore() {
        }
    }
}
