package com.saga.transaction.domain

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "deposits")
class Deposit(
    @Id
    @Column(name = "deposit_id")
    val depositId: String,

    @Column(name = "transaction_id", nullable = false)
    val transactionId: String,

    @Column(name = "account_number", nullable = false)
    val accountNumber: String,

    @Column(nullable = false)
    val amount: BigDecimal,

    @Column(nullable = false)
    var status: String,

    @Column(name = "saga_id")
    val sagaId: String?,

    @Column(name = "created_at")
    val createdAt: LocalDateTime = LocalDateTime.now()
)

