package com.distributedsystem.payment.repository;

import com.distributedsystem.payment.entity.LedgerTransaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface LedgerTransactionRepository extends JpaRepository<LedgerTransaction, String> {
    Optional<LedgerTransaction> findByOrderId(String orderId);
}
