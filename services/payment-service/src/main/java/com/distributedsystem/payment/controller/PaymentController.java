package com.distributedsystem.payment.controller;

import com.distributedsystem.payment.entity.Account;
import com.distributedsystem.payment.entity.LedgerTransaction;
import com.distributedsystem.payment.entity.OutboxEvent;
import com.distributedsystem.payment.repository.AccountRepository;
import com.distributedsystem.payment.repository.LedgerTransactionRepository;
import com.distributedsystem.payment.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/payment")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class PaymentController {

    private final AccountRepository accountRepository;
    private final LedgerTransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;

    @GetMapping("/accounts")
    public ResponseEntity<List<Account>> getAllAccounts() {
        return ResponseEntity.ok(accountRepository.findAll());
    }

    @GetMapping("/accounts/{customerId}")
    public ResponseEntity<Account> getAccountByCustomer(@PathVariable String customerId) {
        return accountRepository.findByCustomerId(customerId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/transactions")
    public ResponseEntity<List<LedgerTransaction>> getAllTransactions() {
        return ResponseEntity.ok(transactionRepository.findAll());
    }

    @GetMapping("/outbox")
    public ResponseEntity<List<OutboxEvent>> getOutboxEvents() {
        return ResponseEntity.ok(outboxEventRepository.findAll());
    }
}
