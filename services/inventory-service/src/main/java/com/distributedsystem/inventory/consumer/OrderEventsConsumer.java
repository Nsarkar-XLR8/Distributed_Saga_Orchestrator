package com.distributedsystem.inventory.consumer;

import com.distributedsystem.inventory.service.InventoryManagementService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventsConsumer {

    private final InventoryManagementService inventoryService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "order-events", groupId = "inventory-service-group")
    public void onOrderEvent(
        @Payload String message,
        @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
        @Header(value = "x-event-type", required = false) String headerEventType
    ) {
        try {
            JsonNode root = objectMapper.readTree(message);
            String eventType = headerEventType != null ? headerEventType : root.path("eventType").asText();
            String eventId = root.path("eventId").asText(null);
            String correlationId = root.path("correlationId").asText(key);

            log.info("Received Kafka message on [order-events]: eventType={}, eventId={}", eventType, eventId);

            switch (eventType) {
                case "ReserveInventoryCommand":
                    inventoryService.handleReserveInventoryCommand(eventId, correlationId, message);
                    break;

                case "ReleaseInventoryCommand":
                    inventoryService.handleReleaseInventoryCommand(eventId, correlationId, message);
                    break;

                default:
                    // Ignore other order events not relevant to inventory
                    break;
            }
        } catch (Exception e) {
            log.error("Error consuming message from order-events: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
