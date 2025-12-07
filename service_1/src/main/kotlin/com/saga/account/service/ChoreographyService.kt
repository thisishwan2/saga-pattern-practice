package com.saga.account.service

import com.saga.account.domain.AccountTransaction
import com.saga.account.domain.SagaState
import com.saga.account.repository.AccountRepository
import com.saga.account.repository.AccountTransactionRepository
import com.saga.account.repository.SagaStateRepository
import com.saga.common.dto.TransferRequest
import com.saga.common.dto.TransferResponse
import com.saga.common.event.WithdrawFailedEvent
import com.saga.common.event.WithdrawSuccessEvent
import jakarta.transaction.Transactional
import mu.KotlinLogging
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class ChoreographyService(
    private val accountRepository: AccountRepository,
    private val accountTransactionRepository: AccountTransactionRepository,
    private val sagaStateRepository: SagaStateRepository,
    private val KafkaTemplate: KafkaTemplate<String, Any>
) {
    private val log = KotlinLogging.logger {}

    @Transactional
    fun initiateTransfer(request: TransferRequest) : TransferResponse {
        log.info("Choreography 방식의 SAGA 트랜잭션 시작")
        val sagaId = UUID.randomUUID().toString()

        try {
            // 1. 출금처리
            val fromAccount = accountRepository.findByAccountNumber(request.fromAccountNumber)
                ?: throw RuntimeException("출금 계좌 없음")

            // 잔액 validation
            // 실패에 대한 이벤트 발행
            if (fromAccount.balance<request.amount) {
                val event = WithdrawFailedEvent(sagaId, request.fromAccountNumber, "잔액 부족")
                KafkaTemplate.send("account.withdraw.failed", event)
                return TransferResponse(sagaId, "FAILED", "잔액 부족")
            }

            fromAccount.balance = fromAccount.balance.subtract(request.amount)
            accountRepository.save(fromAccount)

            // 2. 출금처리 트랜잭션 수행 기록
            val withdrawTx = AccountTransaction(
                transactionId = UUID.randomUUID().toString(),
                accountId = fromAccount.accountId,
                amount = request.amount,
                transactionType = "WITHDRAW",
                sagaId = sagaId,
                status = "COMPLETED"
            )
            accountTransactionRepository.save(withdrawTx)

            // 3. Saga 상태 저장(편의상 AccountService가 Orchestration 역할 수행)
            val sagaState = SagaState(
                sagaId = sagaId,
                patternType = "ORCHESTRATION",
                fromAccountId = fromAccount.accountId,
                toAccountId = request.toAccountNumber,
                amount = request.amount,
                status = "STARTED" // 시작 상태로 설정
            )
            sagaStateRepository.save(sagaState)

            log.info("출금 처리 로컬 트랜잭션 성공");

            // 입금 처리 이벤트 발행
            val event = WithdrawSuccessEvent(
                sagaId = sagaId,
                accountNumber = request.fromAccountNumber,
                toAccountNumber = request.toAccountNumber,
                amount = request.amount
            )
            KafkaTemplate.send("account.withdraw.success", event)

            return TransferResponse(sagaId, "STARTED", "출금 성공, 입금 처리 중")
        }catch (e: Exception) {
            val event = WithdrawFailedEvent(sagaId, request.fromAccountNumber, "잔액 부족")
            KafkaTemplate.send("account.withdraw.failed", event)
            return TransferResponse(sagaId, "FAILED", "잔액 부족")
        }

        // SAGA 트랜잭션 시작 이벤트 발행
        KafkaTemplate.send("saga-transfer-start", "SAGA 트랜잭션 시작 메시지")
    }
}