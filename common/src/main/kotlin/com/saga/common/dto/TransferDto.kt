package com.saga.common.dto

import java.math.BigDecimal

data class TransferRequest(
    val sagaIn: String,
    val toAccountNumber: String,
    val amount: BigDecimal
)

data class TransferResponse(
    val sagaId: String,
    val status: String,
    val message: String
)