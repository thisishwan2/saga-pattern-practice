package com.saga.common.event

import java.math.BigDecimal

data class WithdrawSuccessEvent(
    val sagaId: String,
    val accountNumber: String,
    val toAccountNumber: String,
    val amount: BigDecimal
)

data class WithdrawFailedEvent(
    val sagaId: String,
    val accountNumber: String,
    val reason: String
)

data class DepositSuccessEvent(
    val sagaId: String,
    val accountNumber: String,
    val amount: BigDecimal
)

data class DepositFailedEvent(
    val sagaId: String,
    val accountNumber: String,
    val reason: String
)

