package com.saga.transaction.controller

import com.saga.transaction.service.DepositService
import com.saga.common.dto.DepositRequest
import com.saga.common.dto.DepositResponse
import org.springframework.web.bind.annotation.*

@RestController
// 외부에 노출되지 않고, 내부 서비스간의 통신만 수행하기에 "/internal"로 매핑(실제 운영 환경에서는 ip나 네트워크 제한 설정)
@RequestMapping("/internal")
class DepositController(
    private val depositService: DepositService
) {
    // 출금 서비스로부터 호출을 받는다.
    @PostMapping("/deposit")
    fun processDeposit(@RequestBody request: DepositRequest): DepositResponse {
        return depositService.processDeposit(request)
    }
}

