package com.saga.account.domain

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "saga_state")
class SagaState(
    @Id
    @Column(name = "saga_id")
    val sagaId: String,

    @Column(name = "pattern_type", nullable = false)
    val patternType: String,

    @Column(name = "from_account_id", nullable = false)
    val fromAccountId: String,

    @Column(name = "to_account_id", nullable = false)
    val toAccountId: String,

    @Column(nullable = false)
    val amount: BigDecimal,

    @Column(nullable = false)
    var status: String,

    @Column(name = "created_at")
    val createdAt: LocalDateTime = LocalDateTime.now()
)

