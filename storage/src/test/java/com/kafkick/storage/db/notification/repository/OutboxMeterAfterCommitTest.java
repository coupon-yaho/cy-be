package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>미터는 트랜잭션을 안 탄다.</b> 쓰기 전에 세면 그 트랜잭션이 되돌려져도 지표에는 남고,
 * 숫자를 보는 사람은 <b>일어나지 않은 일</b>을 본다. 만료 회수는 한 트랜잭션에서 여러 행을
 * 고치므로, 뒤쪽 행의 오류가 앞쪽까지 되돌려도 숫자만 그대로다. 리뷰가 짚었다.
 *
 * <p><b>이 테스트가 있는 이유</b> — 처음에는 돌연변이(커밋 전에 세기)가 <b>안 잡혔다.</b>
 * 회수가 {@code REQUIRES_NEW} 안에서 돌아 {@code claimBatch} 가 반환될 때는 이미 커밋된
 * 뒤라, 바깥에서는 커밋 전에 셌는지 뒤에 셌는지 구분할 수 없다. 그래서 그 지연 장치를
 * <b>직접</b> 태운다.
 */
@RepositoryTest
@Import(OutboxMeterTestConfig.class)
// **끄지 않으면 아무것도 못 잰다.** @DataJpaTest 는 테스트마다 트랜잭션을 열고 끝에
// 롤백하므로, 그 안에서는 "커밋 뒤" 가 영영 안 온다 — 세 경우가 전부 0 으로 보인다.
// 형제 outbox 테스트들이 같은 이유로 같은 것을 끈다.
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OutboxMeterAfterCommitTest {

    @Autowired PlatformTransactionManager transactionManager;

    @Test
    @DisplayName("트랜잭션 밖이면 그 자리에서 돈다 — 기다릴 커밋이 없다")
    void runsImmediatelyOutsideATransaction() {
        AtomicInteger ran = new AtomicInteger();

        NotificationOutboxRepositoryImpl.afterCommit(ran::incrementAndGet);

        assertThat(ran).hasValue(1);
    }

    @Test
    @DisplayName("커밋 전에는 안 돈다 — 트랜잭션이 끝나야 센다")
    void waitsForTheCommit() {
        AtomicInteger ran = new AtomicInteger();

        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            NotificationOutboxRepositoryImpl.afterCommit(ran::incrementAndGet);
            assertThat(ran).as("커밋 전에 이미 셌다면 롤백돼도 숫자가 남는다").hasValue(0);
        });

        assertThat(ran).hasValue(1);
    }

    @Test
    @DisplayName("롤백되면 영영 안 돈다 — 일어나지 않은 일이 지표에 남으면 안 된다")
    void neverRunsWhenTheTransactionRollsBack() {
        AtomicInteger ran = new AtomicInteger();

        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            NotificationOutboxRepositoryImpl.afterCommit(ran::incrementAndGet);
            status.setRollbackOnly();
        });

        assertThat(ran)
                .as("되돌려진 회수가 지표에 남으면 숫자를 보는 사람이 없는 일을 본다")
                .hasValue(0);
    }
}
