package com.saga.notification.service

import com.saga.notification.domain.Notification
import com.saga.notification.repository.NotificationRepository
import com.saga.common.dto.NotificationRequest
import com.saga.common.dto.NotificationResponse
import com.saga.common.event.DepositSuccessEvent
import com.saga.common.event.WithdrawFailedEvent
import com.saga.common.event.NotificationFailedEvent
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.*

@Service
class NotificationService(
    private val notificationRepository: NotificationRepository,
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    // Orchestration 방식에서 사용
    @Transactional
    fun sendNotification(request: NotificationRequest): NotificationResponse {
        val notificationId = UUID.randomUUID().toString()

        val notification = Notification(
            notificationId = notificationId,
            userId = request.userId,
            sagaId = request.sagaId,
            notificationType = request.notificationType,
            message = request.message,
            status = "SENT"
        )

        notificationRepository.save(notification)

        // 실제 알람을 전송 or 이메일 전송
        println("[NOTIFICATION] Type: ${request.notificationType}, User: ${request.userId}, Message: ${request.message}")

        return NotificationResponse(notificationId, "SENT")
    }

    // Choreography 방식에서 사용
    @KafkaListener(topics = ["transaction.deposit.success"], groupId = "notification-service-group")
    @Transactional
    fun handleDepositSuccess(event: DepositSuccessEvent) {
        try {
            val notificationId = UUID.randomUUID().toString()

            val notification = Notification(
                notificationId = notificationId,
                userId = event.accountNumber,
                sagaId = event.sagaId,
                notificationType = "DEPOSIT_SUCCESS",
                message = "u received ${event.amount}",
                status = "SENT"
            )

            notificationRepository.save(notification)

            println("[NOTIFICATION] Deposit success notification sent to ${event.accountNumber}")
        } catch (e : Exception) {
            val notificationFailedEvent = NotificationFailedEvent(
                sagaId = event.sagaId,
                accountNumber = event.accountNumber,
                reason = e.message ?: "Notification processing failed"
            )
            // 해당 실패 이벤트는 출금, 입금 서비스에서 수신
            kafkaTemplate.send("notification.failed", notificationFailedEvent)

            println("[NOTIFICATION] Failed to send deposit success notification: ${e.message}")
        }
    }

    // 출금 실패에 대한 알림 발송
    @KafkaListener(topics = ["account.withdraw.failed"], groupId = "notification-service-group")
    @Transactional
    fun handleWithdrawFailed(event: WithdrawFailedEvent) {
        val notification = Notification(
            notificationId = UUID.randomUUID().toString(),
            userId = event.accountNumber,
            sagaId = event.sagaId,
            notificationType = "WITHDRAW_FAILED",
            message = "Withdraw failed: ${event.reason}",
            status = "SENT"
        )

        notificationRepository.save(notification)

        println("[NOTIFICATION] Withdraw failed notification sent to ${event.accountNumber}")
    }

}