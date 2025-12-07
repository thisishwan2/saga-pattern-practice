package com.saga.account.domain

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "accounts")
class Account(
    @Id
    @Column(name = "account_id")
    val accountId: String,

    @Column(name = "account_number", unique = true, nullable = false)
    val accountNumber: String,

    @Column(nullable = false)
    var balance: BigDecimal,

    @Column(nullable = false)
    var status: String = "ACTIVE",

    @Column(name = "created_at")
    val createdAt: LocalDateTime = LocalDateTime.now()
)


