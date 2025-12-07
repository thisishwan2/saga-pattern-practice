package com.saga.transaction.repository

import com.saga.transaction.domain.Deposit
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface DepositRepository : JpaRepository<Deposit, String>

