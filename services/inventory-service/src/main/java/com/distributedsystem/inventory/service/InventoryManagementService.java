package com.distributedsystem.inventory.service;

import com.distributedsystem.inventory.entity.*;
import com.distributedsystem.inventory.repository.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryManagementService {

    private final ProductRepository productRepository;
    private final ReservationRepository reservationRepository;
    private final SagaTombstoneRepository tombstoneRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void handleReserveInventoryCommand(String eventId, String correlationId, String rawPayload) {
        // 1. Consumer Idempotency Guard
        if (eventId != null && processedEventRepository.existsById(eventId)) {
            log.info("⚡ Idempotent duplicate detected for eventId: {}. Skipping reservation.", eventId);
            return;
        }

        try {
            JsonNode root = objectMapper.readTree(rawPayload);
            JsonNode payload = root.has("payload") ? root.get("payload") : root;
            String orderId = payload.get("orderId").asText();

            // 2. Inversion Guard: Check Saga Tombstones (Ghost Reservation Prevention)
            if (orderId != null && tombstoneRepository.existsById(orderId)) {
                log.warn("🛡️ INVERSION GUARD TRIGGERED: Order {} already has a compensation tombstone. Aborting reservation.", orderId);
                emitOutboxEvent(
                    orderId,
                    "InventoryReservationFailedEvent",
                    "inventory-events",
                    correlationId,
                    objectMapper.writeValueAsString(new ReservationFailedPayload(orderId, "PRE_CANCELLED", "Order was pre-cancelled before reservation arrived."))
                );
                recordProcessedEvent(eventId, "inventory-consumer");
                return;
            }

            JsonNode items = payload.get("items");
            boolean allAvailable = true;
            String failedProduct = null;
            int requestedQty = 0;
            int availQty = 0;

            // 3. Pessimistic Row Locking & Stock Availability Check
            for (JsonNode item : items) {
                String productId = item.get("productId").asText();
                int quantity = item.get("quantity").asInt();

                Optional<Product> productOpt = productRepository.findByIdWithLock(productId);
                if (productOpt.isEmpty()) {
                    allAvailable = false;
                    failedProduct = productId;
                    break;
                }

                Product product = productOpt.get();
                int available = product.getTotalStock() - product.getReservedStock();
                if (available < quantity) {
                    allAvailable = false;
                    failedProduct = productId;
                    requestedQty = quantity;
                    availQty = available;
                    break;
                }
            }

            if (!allAvailable) {
                log.warn("❌ Stock unavailable for order {}. Product {} (Requested: {}, Available: {})", orderId, failedProduct, requestedQty, availQty);
                emitOutboxEvent(
                    orderId,
                    "InventoryReservationFailedEvent",
                    "inventory-events",
                    correlationId,
                    objectMapper.writeValueAsString(new ReservationFailedPayload(orderId, "OUT_OF_STOCK", "Product " + failedProduct + " is out of stock."))
                );
                recordProcessedEvent(eventId, "inventory-consumer");
                return;
            }

            // 4. Perform Reservation and Update Stock atomically
            String reservationId = "res_" + UUID.randomUUID().toString().substring(0, 8);
            for (JsonNode item : items) {
                String productId = item.get("productId").asText();
                int quantity = item.get("quantity").asInt();

                Product product = productRepository.findByIdWithLock(productId).orElseThrow();
                product.setReservedStock(product.getReservedStock() + quantity);
                productRepository.save(product);

                Reservation reservation = Reservation.builder()
                    .id(UUID.randomUUID().toString())
                    .orderId(orderId)
                    .product(product)
                    .quantity(quantity)
                    .status("RESERVED")
                    .createdAt(Instant.now())
                    .build();
                if (reservation != null) {
                    reservationRepository.save(reservation);
                }
            }

            // 5. Publish InventoryReservedEvent via Outbox
            emitOutboxEvent(
                reservationId,
                "InventoryReservedEvent",
                "inventory-events",
                correlationId,
                objectMapper.writeValueAsString(new ReservationSuccessPayload(reservationId, orderId, "WH-MAIN-1", items))
            );

            recordProcessedEvent(eventId, "inventory-consumer");
            log.info("✅ Inventory reserved successfully for order: {}", orderId);

        } catch (Exception e) {
            log.error("Error processing ReserveInventoryCommand: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Transactional
    public void handleReleaseInventoryCommand(String eventId, String correlationId, String rawPayload) {
        // 1. Consumer Idempotency Guard
        if (eventId != null && processedEventRepository.existsById(eventId)) {
            log.info("⚡ Idempotent duplicate detected for eventId: {}. Skipping release.", eventId);
            return;
        }

        try {
            JsonNode root = objectMapper.readTree(rawPayload);
            JsonNode payload = root.has("payload") ? root.get("payload") : root;
            String orderId = payload.get("orderId").asText();

            List<Reservation> activeReservations = reservationRepository.findByOrderId(orderId);

            if (activeReservations.isEmpty()) {
                // Inversion Scenario: Release command arrived BEFORE Reserve command!
                log.warn("🛡️ COMPENSATION INVERSION DETECTED: ReleaseInventoryCommand arrived before reservation for order {}. Recording Tombstone.", orderId);
                SagaTombstone tombstone = SagaTombstone.builder()
                    .orderId(orderId)
                    .reason("PRE_CANCELLED_COMPENSATION_INVERSION")
                    .createdAt(Instant.now())
                    .build();
                if (tombstone != null) {
                    tombstoneRepository.save(tombstone);
                }
            } else {
                // Normal Compensation: Release stock back to available pool
                for (Reservation res : activeReservations) {
                    if ("RESERVED".equals(res.getStatus())) {
                        Product product = productRepository.findByIdWithLock(res.getProduct().getId()).orElseThrow();
                        int newReserved = Math.max(0, product.getReservedStock() - res.getQuantity());
                        product.setReservedStock(newReserved);
                        productRepository.save(product);

                        res.setStatus("RELEASED");
                        reservationRepository.save(res);
                    }
                }
                log.info("✅ Released stock reservation for order: {}", orderId);
            }

            // Publish InventoryReleasedEvent via Outbox
            emitOutboxEvent(
                orderId,
                "InventoryReleasedEvent",
                "inventory-events",
                correlationId,
                objectMapper.writeValueAsString(new ReleaseSuccessPayload(orderId, "RELEASED"))
            );

            recordProcessedEvent(eventId, "inventory-consumer");

        } catch (Exception e) {
            log.error("Error processing ReleaseInventoryCommand: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void emitOutboxEvent(String aggregateId, String eventType, String topic, String correlationId, String payloadJson) {
        String outboxId = UUID.randomUUID().toString();
        OutboxEvent outboxEvent = OutboxEvent.builder()
            .id(outboxId)
            .aggregateType("Inventory")
            .aggregateId(aggregateId)
            .eventType(eventType)
            .topic(topic)
            .correlationId(correlationId != null ? correlationId : aggregateId)
            .status("PENDING")
            .payload(formatCloudEventEnvelope(outboxId, eventType, aggregateId, correlationId, payloadJson))
            .createdAt(Instant.now())
            .build();
        if (outboxEvent != null) {
            outboxEventRepository.save(outboxEvent);
        }
    }

    private String formatCloudEventEnvelope(String eventId, String eventType, String aggregateId, String correlationId, String payloadJson) {
        return String.format(
            "{\"eventId\":\"%s\",\"eventType\":\"%s\",\"aggregateType\":\"Inventory\",\"aggregateId\":\"%s\",\"correlationId\":\"%s\",\"timestamp\":\"%s\",\"payload\":%s}",
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
    record ReservationSuccessPayload(String reservationId, String orderId, String warehouseId, Object items) {}
    record ReservationFailedPayload(String orderId, String reason, String message) {}
    record ReleaseSuccessPayload(String orderId, String status) {}
}
