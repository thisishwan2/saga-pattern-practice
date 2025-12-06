package com.saga.common.dto

data class NotificationRequest(
    val sagaId: String,
    val userId: String,
    val notificationType: String,
    val message: String
)

data class NotificationResponse(
    val notificationId: String,
    val status: String
)

