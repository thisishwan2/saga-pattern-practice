package com.saga.notification.domain

import jakarta.persistence.*
import java.time.LocalDateTime

@Entity
@Table(name = "notifications")
class Notification(
    @Id
    @Column(name = "notification_id")
    val notificationId: String,

    @Column(name = "user_id", nullable = false)
    val userId: String,

    @Column(name = "saga_id")
    val sagaId: String?,

    @Column(name = "notification_type", nullable = false)
    val notificationType: String,

    @Column(nullable = false)
    val message: String,

    @Column(nullable = false)
    var status: String,

    @Column(name = "created_at")
    val createdAt: LocalDateTime = LocalDateTime.now()
)