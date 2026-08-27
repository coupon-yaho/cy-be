package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponIssuePolicyValidatorTest {

    private static final Instant ISSUED_AT =
            Instant.parse("2026-08-18T05:30:00Z");

    @Mock
    private CouponRoundRepository couponRoundRepository;

    @Mock
    private IssuanceRepository issuanceRepository;

    @Test
    @DisplayName("조회 트랜잭션이 끝난 뒤 호출자에게 제어를 돌려준다")
    void endsReadOnlyTransactionBeforeReturning() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(TestConfig.class)) {
            CouponIssuePolicyValidator validator = context.getBean(
                    CouponIssuePolicyValidator.class
            );

            validator.validate(command(MembershipGrade.GOLD, ISSUED_AT));

            assertThat(TransactionSynchronizationManager
                    .isActualTransactionActive()).isFalse();
        }
    }

    @Test
    @DisplayName("오픈된 대상 회차이고 미발급 회원이면 사전검증을 통과한다")
    void acceptsEligibleMemberWithoutExistingIssuance() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(CouponRoundStatus.OPEN)));
        when(issuanceRepository.existsForCouponRoundAndMember(10L, 20L))
                .thenReturn(false);

        validator().validate(command(MembershipGrade.GOLD, ISSUED_AT));

        verify(issuanceRepository).existsForCouponRoundAndMember(10L, 20L);
    }

    @Test
    @DisplayName("오픈 전 요청은 중복 발급 조회 전에 거부한다")
    void rejectsBeforeOpenBeforeDuplicateLookup() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(
                        CouponRoundStatus.SCHEDULED
                )));

        assertErrorCode(
                command(
                        MembershipGrade.GOLD,
                        Instant.parse("2026-08-18T04:59:59Z")
                ),
                CouponIssueErrorCode.NOT_OPENED
        );

        verify(issuanceRepository, never())
                .existsForCouponRoundAndMember(10L, 20L);
    }

    @Test
    @DisplayName("마감된 회차는 중복 발급 조회 전에 거부한다")
    void rejectsClosedCampaignBeforeDuplicateLookup() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(CouponRoundStatus.CLOSED)));

        assertErrorCode(
                command(MembershipGrade.GOLD, ISSUED_AT),
                CouponIssueErrorCode.CAMPAIGN_CLOSED
        );

        verify(issuanceRepository, never())
                .existsForCouponRoundAndMember(10L, 20L);
    }

    @Test
    @DisplayName("대상이 아닌 등급은 중복 발급 조회 전에 거부한다")
    void rejectsIneligibleGradeBeforeDuplicateLookup() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(CouponRoundStatus.OPEN)));

        assertErrorCode(
                command(MembershipGrade.SILVER, ISSUED_AT),
                CouponIssueErrorCode.GRADE_NOT_ELIGIBLE
        );

        verify(issuanceRepository, never())
                .existsForCouponRoundAndMember(10L, 20L);
    }

    @Test
    @DisplayName("이미 같은 회차를 발급받은 회원은 사전검증에서 거부한다")
    void rejectsExistingIssuance() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(CouponRoundStatus.OPEN)));
        when(issuanceRepository.existsForCouponRoundAndMember(10L, 20L))
                .thenReturn(true);

        assertErrorCode(
                command(MembershipGrade.GOLD, ISSUED_AT),
                CouponIssueErrorCode.ALREADY_ISSUED
        );
    }

    @Test
    @DisplayName("존재하지 않는 회차는 중복 발급 조회 전에 거부한다")
    void rejectsMissingCouponRoundBeforeDuplicateLookup() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.empty());

        assertErrorCode(
                command(MembershipGrade.GOLD, ISSUED_AT),
                CouponIssueErrorCode.COUPON_ROUND_NOT_FOUND
        );

        verify(issuanceRepository, never())
                .existsForCouponRoundAndMember(10L, 20L);
    }

    @Test
    @DisplayName("필수 요청 값이 없으면 Repository 조회 전에 거부한다")
    void rejectsInvalidCommandBeforeRepositoryLookup() {
        CouponIssueCommand invalidCommand = new CouponIssueCommand(
                10L,
                null,
                MembershipGrade.GOLD,
                "request-1",
                ISSUED_AT
        );

        assertErrorCode(
                invalidCommand,
                CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST
        );

        verifyNoInteractions(couponRoundRepository, issuanceRepository);
    }

    private CouponIssuePolicyValidator validator() {
        return new CouponIssuePolicyValidator(
                couponRoundRepository,
                issuanceRepository
        );
    }

    private void assertErrorCode(
            CouponIssueCommand command,
            CouponIssueErrorCode errorCode
    ) {
        assertThatThrownBy(() -> validator().validate(command))
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(errorCode)
                );
    }

    private static CouponIssueCommand command(
            MembershipGrade grade,
            Instant issuedAt
    ) {
        return new CouponIssueCommand(
                10L,
                20L,
                grade,
                "request-1",
                issuedAt
        );
    }

    private static CouponRound couponRound(CouponRoundStatus status) {
        return CouponRound.restore(
                10L,
                1L,
                1L,
                "골드 VIP 5천원 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP),
                Instant.parse("2026-08-18T05:00:00Z"),
                Instant.parse("2026-08-18T07:00:00Z"),
                status,
                Instant.parse("2026-08-17T00:00:00Z")
        );
    }

    @Configuration
    @EnableTransactionManagement
    static class TestConfig {

        @Bean
        PlatformTransactionManager transactionManager() {
            return new TestTransactionManager();
        }

        @Bean
        CouponRoundRepository couponRoundRepository() {
            CouponRoundRepository repository = mock(
                    CouponRoundRepository.class
            );
            when(repository.findById(10L)).thenAnswer(invocation -> {
                assertThat(TransactionSynchronizationManager
                        .isActualTransactionActive()).isTrue();
                assertThat(TransactionSynchronizationManager
                        .isCurrentTransactionReadOnly()).isTrue();
                return Optional.of(couponRound(CouponRoundStatus.OPEN));
            });
            return repository;
        }

        @Bean
        IssuanceRepository issuanceRepository() {
            IssuanceRepository repository = mock(IssuanceRepository.class);
            when(repository.existsForCouponRoundAndMember(10L, 20L))
                    .thenAnswer(invocation -> {
                        assertThat(TransactionSynchronizationManager
                                .isActualTransactionActive()).isTrue();
                        return false;
                    });
            return repository;
        }

        @Bean
        CouponIssuePolicyValidator couponIssuePolicyValidator(
                CouponRoundRepository couponRoundRepository,
                IssuanceRepository issuanceRepository
        ) {
            return new CouponIssuePolicyValidator(
                    couponRoundRepository,
                    issuanceRepository
            );
        }
    }

    static class TestTransactionManager
            extends AbstractPlatformTransactionManager {

        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(
                Object transaction,
                TransactionDefinition definition
        ) {
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }
    }
}
