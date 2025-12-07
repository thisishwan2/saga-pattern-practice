package com.saga.transaction.service


import com.saga.transaction.domain.Deposit
import com.saga.transaction.domain.Transaction
import com.saga.transaction.repository.DepositRepository
import com.saga.transaction.repository.TransactionRepository
import com.saga.common.dto.DepositRequest
import com.saga.common.dto.DepositResponse
import com.saga.common.dto.NotificationRequest
import com.saga.common.dto.NotificationResponse
import com.saga.common.event.DepositFailedEvent
import com.saga.common.event.DepositSuccessEvent
import com.saga.common.event.WithdrawSuccessEvent
import com.saga.common.event.NotificationFailedEvent
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.client.RestTemplate
import java.util.*

@Service
class DepositService(
    private val transactionRepository: TransactionRepository,
    private val depositRepository: DepositRepository,
    private val kafkaTemplate: KafkaTemplate<String, Any>,
    private val restTemplate: RestTemplate,
    @Value("\${service.notification.url}") private val notificationServiceUrl: String
) {
    private val log = KotlinLogging.logger {}

    // Orchestration 방식에서 사용
    // 출금 서비스로부터 호출을 받아 입금 처리
    @Transactional
    fun processDeposit(request: DepositRequest): DepositResponse {
        try {
            log.info("입급 처리 로컬 트랜잭션 시작")
            // 입금 처리 로컬 트랜잭션
            val transactionId = UUID.randomUUID().toString()
            val depositId = UUID.randomUUID().toString()

            val transaction = Transaction(
                transactionId = transactionId,
                sagaId = request.sagaId,
                fromAccountNumber = request.fromAccountNumber,
                toAccountNumber = request.accountNumber,
                amount = request.amount,
                status = "COMPLETED"
            )
            transactionRepository.save(transaction)

            val deposit = Deposit(
                depositId = depositId,
                transactionId = transactionId,
                accountNumber = request.accountNumber,
                amount = request.amount,
                status = "COMPLETED",
                sagaId = request.sagaId
            )
            depositRepository.save(deposit)
            log.info("입급 처리 로컬 트랜잭션 성공")

            // 알림 서비스 호출
            try {
                val notificationRequest = NotificationRequest(
                    sagaId = request.sagaId,
                    userId = request.accountNumber,
                    notificationType = "DEPOSIT_SUCCESS",
                    message = "Received ${request.amount} from ${request.fromAccountNumber}"
                )
                log.info("알림 서비스 호출")
                restTemplate.postForObject(
                    "$notificationServiceUrl/internal/notification",
                    notificationRequest,
                    NotificationResponse::class.java
                )
            } catch (e : Exception) {
                // 알림 발송이 실패한 경우는 보상 처리를 하지 않아도 됨.
                // 만약 알림 실패가 입금, 출금도 되돌려야 한다면 보상 수행
                log.info("알림 서비스 호출 실패: ${e.message}")
            }

            return DepositResponse(depositId, "COMPLETED")

        } catch (e : Exception) {
            throw RuntimeException("입금 실패", e)
        }
    }

    // Choreography 방식에서 사용
    // 출금 로컬 트랜잭션 성공 이벤트를 수신하여 입금 처리
    @KafkaListener(topics = ["account.withdraw.success"], groupId = "transaction-service-group")
    @Transactional
    fun handleWithdrawSuccess(event: WithdrawSuccessEvent) {
        try {
            log.info("출금 성공 이벤트 수신: 입금 처리 로컬 트랜잭션 시작")
            val transactionId = UUID.randomUUID().toString()
            val depositId = UUID.randomUUID().toString()

            val transaction = Transaction(
                transactionId = transactionId,
                sagaId = event.sagaId,
                fromAccountNumber = event.accountNumber,
                toAccountNumber = event.toAccountNumber,
                amount = event.amount,
                status = "COMPLETED"
            )
            transactionRepository.save(transaction)

            val deposit = Deposit(
                depositId = depositId,
                transactionId = transactionId,
                accountNumber = event.toAccountNumber,
                amount = event.amount,
                status = "COMPLETED",
                sagaId = event.sagaId
            )
            depositRepository.save(deposit)

            // 입금 로컬 트랜잭션 처리 성공에 따른 이벤트 발행
            val depositSuccessEvent = DepositSuccessEvent(
                sagaId = event.sagaId,
                accountNumber = event.toAccountNumber,
                amount = event.amount
            )
            kafkaTemplate.send("transaction.deposit.success", depositSuccessEvent)
            log.info("입금 처리 로컬 트랜잭션 처리 성공: 입금 성공 이벤트 발행")
        } catch (e : Exception) {
            // DLQ 패턴을 통해 보상 트랜잭션 구현도 가능
            val depositFailedEvent = DepositFailedEvent(
                sagaId = event.sagaId,
                accountNumber = event.toAccountNumber,
                reason = e.message ?: "Unknown error"
            )
            // 입금 실패 이벤트 발행 -> 출금 서비스 수신
            log.info("입금 처리 로컬 트랜잭션 처리 실패: 입금 실패 이벤트 발행")
            kafkaTemplate.send("transaction.deposit.failed", depositFailedEvent)
        }
    }

    // Choreography 방식에서 사용
    // 알림 발송 실패 이벤트 수신
    // 보상 트랜잭션 수행
    @KafkaListener(topics = ["notification.failed"], groupId = "transaction-service-group")
    fun handleNotificationFailed(event: NotificationFailedEvent) {
        log.info("[TRANSACTION] Notification failed for saga ${event.sagaId}: ${event.reason}")
    }

}