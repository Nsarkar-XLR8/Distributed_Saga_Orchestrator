package com.distributedsystem.payment.repository;

import com.distributedsystem.payment.entity.Account;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AccountRepository extends JpaRepository<Account, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT a FROM Account a WHERE a.customerId = :customerId")
    Optional<Account> findByCustomerIdWithLock(@Param("customerId") String customerId);

    Optional<Account> findByCustomerId(String customerId);
}
