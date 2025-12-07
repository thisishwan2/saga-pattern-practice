package com.saga.account.domain


import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "account_transactions")
class AccountTransaction(
    @Id
    @Column(name = "transaction_id")
    val transactionId: String,

    @Column(name = "account_id", nullable = false)
    val accountId: String,

    @Column(nullable = false)
    val amount: BigDecimal,

    @Column(name = "transaction_type", nullable = false)
    val transactionType: String,

    @Column(name = "saga_id")
    val sagaId: String?,

    @Column(nullable = false)
    var status: String,

    @Column(name = "created_at")
    val createdAt: LocalDateTime = LocalDateTime.now()
)

