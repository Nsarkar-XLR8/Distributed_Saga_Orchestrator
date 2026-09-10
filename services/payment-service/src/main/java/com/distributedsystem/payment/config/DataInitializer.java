package com.distributedsystem.payment.config;

import com.distributedsystem.payment.entity.Account;
import com.distributedsystem.payment.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class DataInitializer implements CommandLineRunner {

    private final AccountRepository accountRepository;

    @Override
    public void run(String... args) {
        if (accountRepository.count() == 0) {
            log.info("Seeding customer accounts into payment database...");
            List<Account> seedAccounts = List.of(
                Account.builder().id("acc_cust_5001").customerId("cust_5001").balance(new BigDecimal("500.00")).currency("USD").updatedAt(Instant.now()).build(),
                Account.builder().id("acc_cust_5002").customerId("cust_5002").balance(new BigDecimal("1000.00")).currency("USD").updatedAt(Instant.now()).build(),
                Account.builder().id("acc_cust_insufficient").customerId("cust_insufficient").balance(new BigDecimal("5.00")).currency("USD").updatedAt(Instant.now()).build(),
                Account.builder().id("acc_system_revenue").customerId("system_revenue").balance(new BigDecimal("0.00")).currency("USD").updatedAt(Instant.now()).build()
            );
            for (Account acc : seedAccounts) {
                if (acc != null) {
                    accountRepository.save(acc);
                }
            }
            log.info("Successfully seeded {} customer accounts.", seedAccounts.size());
        }
    }
}
