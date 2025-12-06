package com.saga.common.dto

import java.math.BigDecimal

data class DepositRequest(
    val sagaId: String,
    val accountNumber: String,
    val amount: BigDecimal,
    val fromAccountNumber: String
)

data class DepositResponse(
    val depositId: String,
    val status: String
)

