package com.distributedsystem.payment.service;

import com.distributedsystem.payment.entity.*;
import com.distributedsystem.payment.repository.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentProcessingService {

    private final AccountRepository accountRepository;
    private final LedgerTransactionRepository transactionRepository;
    private final LedgerEntryRepository entryRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void handleInitiatePaymentCommand(String eventId, String correlationId, String rawPayload) {
        // 1. Consumer Idempotency Guard
        if (eventId != null && processedEventRepository.existsById(eventId)) {
            log.info("⚡ Idempotent duplicate detected for payment eventId: {}. Skipping execution.", eventId);
            return;
        }

        try {
            JsonNode root = objectMapper.readTree(rawPayload);
            JsonNode payload = root.has("payload") ? root.get("payload") : root;
            String orderId = payload.get("orderId").asText();
            String customerId = payload.get("customerId").asText();
            BigDecimal amount = new BigDecimal(payload.get("amount").asText());

            // 2. Double-Entry Ledger Concurrency Guard: Lock customer account row
            Optional<Account> customerAccountOpt = accountRepository.findByCustomerIdWithLock(customerId);

            if (customerAccountOpt.isEmpty()) {
                log.warn("❌ Customer account {} does not exist. Failing payment for order {}.", customerId, orderId);
                recordFailedPayment(orderId, customerId, amount, correlationId, "ACCOUNT_NOT_FOUND");
                recordProcessedEvent(eventId, "payment-consumer");
                return;
            }

            Account customerAccount = customerAccountOpt.get();

            // 3. Balance Sufficiency Verification
            if (customerAccount.getBalance().compareTo(amount) < 0) {
                log.warn("❌ Insufficient funds for customer {} (Balance: ${}, Required: ${}). Failing payment for order {}.",
                    customerId, customerAccount.getBalance(), amount, orderId);
                recordFailedPayment(orderId, customerId, amount, correlationId, "INSUFFICIENT_FUNDS");
                recordProcessedEvent(eventId, "payment-consumer");
                return;
            }

            // 4. Double-Entry Execution: Debit Customer, Credit System Revenue
            customerAccount.setBalance(customerAccount.getBalance().subtract(amount));
            customerAccount.setUpdatedAt(Instant.now());
            accountRepository.save(customerAccount);

            Account systemAccount = accountRepository.findByCustomerIdWithLock("system_revenue")
                .orElseGet(() -> accountRepository.save(Account.builder()
                    .id("acc_system_revenue")
                    .customerId("system_revenue")
                    .balance(BigDecimal.ZERO)
                    .currency("USD")
                    .updatedAt(Instant.now())
                    .build()));

            systemAccount.setBalance(systemAccount.getBalance().add(amount));
            systemAccount.setUpdatedAt(Instant.now());
            accountRepository.save(systemAccount);

            // Record Ledger Transaction & Entries
            String transactionId = "tx_" + UUID.randomUUID().toString().substring(0, 8);
            LedgerTransaction transaction = LedgerTransaction.builder()
                .id(transactionId)
                .orderId(orderId)
                .amount(amount)
                .status("SUCCESS")
                .createdAt(Instant.now())
                .build();
            transactionRepository.save(transaction);

            // Debit Entry (Customer)
            entryRepository.save(LedgerEntry.builder()
                .id(UUID.randomUUID().toString())
                .transaction(transaction)
                .account(customerAccount)
                .entryType("DEBIT")
                .amount(amount)
                .createdAt(Instant.now())
                .build());

            // Credit Entry (Revenue)
            entryRepository.save(LedgerEntry.builder()
                .id(UUID.randomUUID().toString())
                .transaction(transaction)
                .account(systemAccount)
                .entryType("CREDIT")
                .amount(amount)
                .createdAt(Instant.now())
                .build());

            // 5. Emit PaymentCapturedEvent via Outbox
            emitOutboxEvent(
                transactionId,
                "PaymentCapturedEvent",
                "payment-events",
                correlationId,
                objectMapper.writeValueAsString(new PaymentSuccessPayload(transactionId, orderId, customerId, amount, "USD", "SUCCESS"))
            );

            recordProcessedEvent(eventId, "payment-consumer");
            log.info("✅ Payment of ${} captured successfully for order: {} (tx: {})", amount, orderId, transactionId);

        } catch (Exception e) {
            log.error("Error processing InitiatePaymentCommand: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void recordFailedPayment(String orderId, String customerId, BigDecimal amount, String correlationId, String failureReason) throws Exception {
        String transactionId = "tx_" + UUID.randomUUID().toString().substring(0, 8);
        LedgerTransaction transaction = LedgerTransaction.builder()
            .id(transactionId)
            .orderId(orderId)
            .amount(amount)
            .status("FAILED")
            .createdAt(Instant.now())
            .build();
        transactionRepository.save(transaction);

        emitOutboxEvent(
            transactionId,
            "PaymentFailedEvent",
            "payment-events",
            correlationId,
            objectMapper.writeValueAsString(new PaymentFailedPayload(transactionId, orderId, customerId, amount, failureReason, "FAILED"))
        );
    }

    private void emitOutboxEvent(String aggregateId, String eventType, String topic, String correlationId, String payloadJson) {
        String outboxId = UUID.randomUUID().toString();
        OutboxEvent outboxEvent = OutboxEvent.builder()
            .id(outboxId)
            .aggregateType("Payment")
            .aggregateId(aggregateId)
            .eventType(eventType)
            .topic(topic)
            .correlationId(correlationId != null ? correlationId : aggregateId)
            .status("PENDING")
            .payload(formatCloudEventEnvelope(outboxId, eventType, aggregateId, correlationId, payloadJson))
            .createdAt(Instant.now())
            .build();
        outboxEventRepository.save(outboxEvent);
    }

    private String formatCloudEventEnvelope(String eventId, String eventType, String aggregateId, String correlationId, String payloadJson) {
        return String.format(
            "{\"eventId\":\"%s\",\"eventType\":\"%s\",\"aggregateType\":\"Payment\",\"aggregateId\":\"%s\",\"correlationId\":\"%s\",\"timestamp\":\"%s\",\"payload\":%s}",
            eventId, eventType, aggregateId, correlationId != null ? correlationId : aggregateId, Instant.now(), payloadJson
        );
    }

    private void recordProcessedEvent(String eventId, String consumerName) {
        if (eventId != null) {
            ProcessedEvent pe = ProcessedEvent.builder()
                .eventId(eventId)
                .consumerName(consumerName)
                .processedAt(Instant.now())
                .build();
            if (pe != null) {
                processedEventRepository.save(pe);
            }
        }
    }

    // Payload DTOs
    record PaymentSuccessPayload(String transactionId, String orderId, String customerId, BigDecimal amount, String currency, String status) {}
    record PaymentFailedPayload(String transactionId, String orderId, String customerId, BigDecimal amount, String failureReason, String status) {}
}
