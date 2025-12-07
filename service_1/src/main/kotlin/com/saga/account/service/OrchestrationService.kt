package com.saga.account.service

import com.saga.account.domain.AccountTransaction
import com.saga.account.domain.SagaState
import com.saga.account.repository.AccountRepository
import com.saga.account.repository.AccountTransactionRepository
import com.saga.account.repository.SagaStateRepository
import com.saga.common.dto.DepositRequest
import com.saga.common.dto.DepositResponse
import com.saga.common.dto.TransferRequest
import com.saga.common.dto.TransferResponse
import jakarta.transaction.Transactional
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.util.UUID

@Service
class OrchestrationService(
    private val accountRepository: AccountRepository,
    private val accountTransactionRepository: AccountTransactionRepository,
    private val sagaStateRepository: SagaStateRepository,
    private val restTemplate: RestTemplate,
    // Orchestration은 REST 호출로 각 서비스와 통신
    @Value("\${service.transaction.url}") private val transactionServiceUrl: String,
    @Value("\${service.notification.url}") private val notificationServiceUrl: String
) {
    private val log = KotlinLogging.logger {}

    @Transactional
    fun executeTransfer(request: TransferRequest) : TransferResponse {
        log.info("Orchestration 방식의 SAGA 트랜잭션 시작")
        val sagaId = UUID.randomUUID().toString()

        try {

            log.info("출금 처리 로컬 트랜잭션 시작");
            // 1. 출금처리
            val fromAccount = accountRepository.findByAccountNumber(request.fromAccountNumber)
                ?: throw RuntimeException("출금 계좌 없음")

            // 잔액 validation
            if (fromAccount.balance<request.amount) {
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

            // 4. 입금 서비스 호출(Transaction Service)
            try {
                val depositRequest = DepositRequest(
                    sagaId = sagaId,
                    accountNumber = request.toAccountNumber,
                    amount = request.amount,
                    fromAccountNumber = request.fromAccountNumber
                )

                restTemplate.postForObject(
                    "$transactionServiceUrl/internal/deposit",
                    depositRequest,
                    DepositResponse::class.java
                ) ?: throw RuntimeException("입금 실패")

                sagaState.status = "COMPLETED"
                sagaStateRepository.save(sagaState)

                return TransferResponse(sagaId, "COMPLETED", "이체 성공")
            }catch (e: Exception) {
                // 출금 서비스 보상 트랜잭션 수행
                fromAccount.balance = fromAccount.balance.add(request.amount)
                accountRepository.save(fromAccount)

                withdrawTx.status = "COMPENSATED"
                accountTransactionRepository.save(withdrawTx)

                sagaState.status = "COMPENSATED"
                sagaStateRepository.save(sagaState)

                return TransferResponse(sagaId, "FAILED", "출금 실패: ${e.message}")
            }

        }catch (e: Exception) {
            return TransferResponse(sagaId, "FAILED", e.message ?: "오류 발생")
        }
    }
}