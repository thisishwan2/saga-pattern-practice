package com.saga.account.service

import com.saga.account.repository.AccountRepository
import com.saga.account.repository.SagaStateRepository
import com.saga.common.event.DepositFailedEvent
import com.saga.common.event.DepositSuccessEvent
import com.saga.common.event.NotificationFailedEvent
import jakarta.transaction.Transactional
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

/**
 * Choreography 패턴의 보상 트랜잭션 서비스
 * 카프카 리스너를 통해 보상 트랜잭션의 이벤트를 수신하고 처리한다.
 */

@Service
class CompensateService(
    private val sagaStateRepository: SagaStateRepository,
    private val accountRepository: AccountRepository
) {
    private val log = KotlinLogging.logger {}

    // 입금 실패 이벤트 수신
    @KafkaListener(topics = ["transaction.deposit.failed"], groupId = "account-service-group")
    fun handleDepositFailed(event: DepositFailedEvent) {
        log.info("입급 실패 이벤트 수신: 출금 보상 처리 수행")
        compensateWithdraw(event.sagaId)
    }

    @Transactional
    fun compensateWithdraw(sagaId: String) {
        val sagaState = sagaStateRepository.findById(sagaId).orElse(null) ?: return

        val fromAccount = accountRepository.findById(sagaState.fromAccountId).orElse(null) ?: return

        // 출금 보상처리: 출금된 금액을 다시 계좌에 더함
        fromAccount.balance = fromAccount.balance.add(sagaState.amount)
        accountRepository.save(fromAccount)

        sagaState.status = "COMPENSATED"
        sagaStateRepository.save(sagaState)
    }

    // 입금 성공 이벤트 수신
    @Transactional
    @KafkaListener(topics = ["transaction.deposit.success"], groupId = "account-service-group")
    fun handleDepositSuccess(event: DepositSuccessEvent) {
        log.info("입급 성공 이벤트 수신: SAGA 완료 처리")
        val sagaState = sagaStateRepository.findById(event.sagaId).orElse(null)
        sagaState?.let {
            it.status = "COMPLETED"
            sagaStateRepository.save(it)
        }
    }

    // 알림 발송 실패 이벤트 수신
    @Transactional
    @KafkaListener(topics = ["notification.failed"], groupId = "account-service-group")
    fun handleNotificationFailed(event: NotificationFailedEvent) {
        log.info("알림 발송 실패 이벤트 수신: SAGA 상태 변경")
        val sagaState = sagaStateRepository.findById(event.sagaId).orElse(null)
        sagaState?.let {
            it.status = "COMPLETED_WITH_NOTIFICATION_FAILURE"
            sagaStateRepository.save(it)
        }
    }

}