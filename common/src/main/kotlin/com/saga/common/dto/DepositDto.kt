package com.saga.common.dto

import java.math.BigDecimal

data class DepositRequest(
    // 분산 트랜잭션 추적을 위한 sagaId(하나의 논리적인 작업 단위)
    // 하나의 논리적인 작업에 대한 각 서비스의 로컬 트랜잭션이 하나의 sagaId로 관리된다.
    val sagaId: String,
    val accountNumber: String,
    val amount: BigDecimal,
    val fromAccountNumber: String
)

data class DepositResponse(
    val depositId: String,
    val status: String
)