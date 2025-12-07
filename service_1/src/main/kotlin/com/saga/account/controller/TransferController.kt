package com.saga.account.controller

import com.saga.account.service.ChoreographyService
import com.saga.account.service.OrchestrationService
import com.saga.common.dto.TransferRequest
import com.saga.common.dto.TransferResponse
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api")
class TransferController(
    private val orchestrationService: OrchestrationService,
    private val choreographyService: ChoreographyService
) {

    // orchestration 패턴 수행
    @PostMapping("/orchestration/transfer")
    fun orchestrationTransfer(@RequestBody request: TransferRequest): TransferResponse {
        return orchestrationService.executeTransfer(request)
    }

    // choreography 패턴 수행
    @PostMapping("/choreography/transfer")
    fun choreographyTransfer(@RequestBody request: TransferRequest): TransferResponse {
        return choreographyService.initiateTransfer(request)
    }
}