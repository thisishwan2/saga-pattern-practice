package com.saga.account.repository

import com.saga.account.domain.Account
import com.saga.account.domain.SagaState
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface SagaStateRepository : JpaRepository<SagaState, String>