package com.saga.notification.controller

import com.saga.notification.service.NotificationService
import com.saga.common.dto.NotificationRequest
import com.saga.common.dto.NotificationResponse
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/internal")
class NotificationController(
    private val notificationService: NotificationService
) {

    @PostMapping("/notification")
    fun sendNotification(@RequestBody request: NotificationRequest): NotificationResponse {
        return notificationService.sendNotification(request)
    }
}
