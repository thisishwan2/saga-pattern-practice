package com.saga.transaction.domain

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "transactions")
class Transaction(
    @Id
    @Column(name = "transaction_id")
    val transactionId: String,

    @Column(name = "saga_id", nullable = false)
    val sagaId: String,

    @Column(name = "from_account_number", nullable = false)
    val fromAccountNumber: String,

    @Column(name = "to_account_number", nullable = false)
    val toAccountNumber: String,

    @Column(nullable = false)
    val amount: BigDecimal,

    @Column(nullable = false)
    var status: String,

    @Column(name = "created_at")
    val createdAt: LocalDateTime = LocalDateTime.now()
)

