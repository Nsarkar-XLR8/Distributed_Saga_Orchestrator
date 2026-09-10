package com.distributedsystem.payment.service;

import com.distributedsystem.payment.entity.OutboxEvent;
import com.distributedsystem.payment.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxRelayService {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 400)
    public void pollAndRelayOutbox() {
        // Phase 1: Claim batch using 2-phase status lease in SQL
        List<OutboxEvent> claimedEvents = claimBatch();
        if (claimedEvents.isEmpty()) {
            return;
        }

        log.info("Claimed {} outbox event(s) in payment-service for asynchronous relay.", claimedEvents.size());

        // Phase 2: Dispatch to Kafka asynchronously OUTSIDE of open database transaction locks
        for (OutboxEvent event : claimedEvents) {
            if (event == null) {
                continue;
            }

            final String topic = event.getTopic();
            final String key = event.getAggregateId();
            final String data = event.getPayload();
            final String eventId = event.getId();
            final String eventType = event.getEventType();

            if (topic == null || key == null || data == null) {
                continue;
            }

            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, data);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    if (eventId != null) {
                        markProcessed(eventId);
                    }
                    log.info("✅ Payment outbox event {} [{}] acknowledged by Kafka", eventId, eventType);
                } else {
                    if (eventId != null) {
                        handleFailure(eventId, ex.getMessage());
                    }
                    log.error("❌ Failed to relay payment outbox event {}: {}", eventId, ex.getMessage());
                }
            });
        }
    }

    @Transactional
    public List<OutboxEvent> claimBatch() {
        return outboxEventRepository.claimLeasedBatch(50);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markProcessed(String eventId) {
        if (eventId == null) return;
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            if (event != null) {
                event.setStatus("PROCESSED");
                event.setProcessedAt(Instant.now());
                event.setLeaseExpiresAt(null);
                outboxEventRepository.save(event);
            }
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void handleFailure(String eventId, String errorMessage) {
        if (eventId == null) return;
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            if (event != null) {
                int nextRetries = (event.getRetryCount() != null ? event.getRetryCount() : 0) + 1;
                event.setRetryCount(nextRetries);
                event.setLastError(errorMessage);
                event.setStatus(nextRetries >= 5 ? "FAILED" : "PENDING");
                event.setLeaseExpiresAt(null);
                outboxEventRepository.save(event);
            }
        });
    }
}
