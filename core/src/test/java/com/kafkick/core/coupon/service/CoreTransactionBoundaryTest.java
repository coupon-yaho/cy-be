package com.kafkick.core.coupon.service;

import java.lang.reflect.Method;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.service.idempotency.IdempotencyClaimService;
import com.kafkick.core.coupon.service.idempotency.IdempotencyExecutionService;
import com.kafkick.core.coupon.service.idempotency.IdempotentOperationService;
import com.kafkick.core.coupontemplate.service.CouponTemplateActivationService;
import com.kafkick.core.coupontemplate.service.CouponTemplateCreateService;
import com.kafkick.core.coupontemplate.service.CouponTemplateQueryService;
import com.kafkick.core.coupontemplate.service.CouponTemplateUpdateService;
import org.springframework.transaction.annotation.Propagation;

import static org.assertj.core.api.Assertions.assertThat;

class CoreTransactionBoundaryTest {

    @Test
    @DisplayName("쿠폰 변경 유즈케이스의 트랜잭션 경계는 core service에 둔다")
    void mutationUseCasesOwnTransactionBoundary() throws Exception {
        assertTransactional(CouponTemplateCreateService.class, "create");
        assertTransactional(CouponTemplateUpdateService.class, "update");
        assertTransactional(
                CouponTemplateActivationService.class,
                "changeActivation"
        );
        assertTransactional(CouponIssueService.class, "issue");
        assertTransactional(CouponUseService.class, "use");
        assertTransactional(CouponCancelUseService.class, "cancelUse");
        assertTransactional(CouponCancelService.class, "cancel");
        assertTransactional(IdempotentOperationService.class, "execute");
    }

    @Test
    @DisplayName("쿠폰 조회 유즈케이스는 core service의 읽기 전용 트랜잭션을 사용한다")
    void queryUseCasesOwnReadOnlyTransactionBoundary() throws Exception {
        assertReadOnly(CouponTemplateQueryService.class, "findById");
        assertReadOnly(CouponTemplateQueryService.class, "findPage");
        assertReadOnly(MemberCouponQueryService.class, "findPage");
        assertReadOnly(MemberCouponQueryService.class, "findOne");
        assertReadOnly(CouponRoundDetailQueryService.class, "findById");
        assertReadOnly(PublicCouponRoundQueryService.class, "findPage");
        assertReadOnly(CouponIssuePolicyValidator.class, "validate");
    }

    @Test
    @DisplayName("회차와 초기 재고 생성은 core 서비스 트랜잭션에서 원자적으로 처리한다")
    void roundCreationOwnsTransactionBoundary() throws Exception {
        Transactional transactional = findMethod(
                CouponRoundCreationService.class,
                "create"
        ).getAnnotation(Transactional.class);

        assertThat(transactional).isNotNull();
        assertThat(transactional.propagation())
                .isEqualTo(Propagation.REQUIRED);
    }

    @Test
    @DisplayName("멱등 선점과 회수는 짧은 독립 트랜잭션으로 실행한다")
    void idempotencyClaimsUseRequiresNewTransactions() throws Exception {
        for (String methodName : java.util.List.of(
                "tryStart",
                "findByKey",
                "tryReclaim",
                "release"
        )) {
            Transactional transactional = findMethod(
                    IdempotencyClaimService.class,
                    methodName
            ).getAnnotation(Transactional.class);
            assertThat(transactional).isNotNull();
            assertThat(transactional.propagation())
                    .isEqualTo(Propagation.REQUIRES_NEW);
        }
    }

    @Test
    @DisplayName("멱등 완료 대기는 외부 트랜잭션 없이 실행한다")
    void idempotencyExecutionRejectsOuterTransaction() throws Exception {
        for (String methodName : java.util.List.of(
                "execute",
                "executeWithMetadata"
        )) {
            Transactional transactional = findMethod(
                    IdempotencyExecutionService.class,
                    methodName
            ).getAnnotation(Transactional.class);

            assertThat(transactional)
                    .as("IdempotencyExecutionService.%s", methodName)
                    .isNotNull();
            assertThat(transactional.propagation())
                    .as("IdempotencyExecutionService.%s", methodName)
                    .isEqualTo(Propagation.NEVER);
        }
    }

    @Test
    @DisplayName("발급 orchestration 자체는 외부 트랜잭션을 열지 않는다")
    void couponIssueOrchestrationHasNoTransactionBoundary() throws Exception {
        assertThat(CouponOperationExecutionService.class.getAnnotation(
                Transactional.class
        )).isNull();
        assertThat(CouponOperationExecutionService.class.getDeclaredMethod(
                "issueWithMetadata",
                Long.class,
                Long.class,
                com.kafkick.core.membership.domain.MembershipGrade.class,
                String.class,
                IssueAttemptCallback.class
        ).getAnnotation(Transactional.class)).isNull();
    }

    private static void assertTransactional(
            Class<?> serviceType,
            String methodName
    ) throws Exception {
        Transactional transactional = findMethod(serviceType, methodName)
                .getAnnotation(Transactional.class);
        assertThat(transactional)
                .as("%s.%s", serviceType.getSimpleName(), methodName)
                .isNotNull();
        assertThat(transactional.readOnly()).isFalse();
    }

    private static void assertReadOnly(
            Class<?> serviceType,
            String methodName
    ) throws Exception {
        Transactional transactional = findMethod(serviceType, methodName)
                .getAnnotation(Transactional.class);
        assertThat(transactional)
                .as("%s.%s", serviceType.getSimpleName(), methodName)
                .isNotNull();
        assertThat(transactional.readOnly()).isTrue();
    }

    private static Method findMethod(
            Class<?> serviceType,
            String methodName
    ) {
        return java.util.Arrays.stream(serviceType.getDeclaredMethods())
                .filter(method -> method.getName().equals(methodName))
                .findFirst()
                .orElseThrow();
    }
}
