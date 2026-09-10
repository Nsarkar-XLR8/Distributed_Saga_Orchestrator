package com.distributedsystem.payment.consumer;

import com.distributedsystem.payment.service.PaymentProcessingService;
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
public class PaymentEventsConsumer {

    private final PaymentProcessingService paymentService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = {"order-events", "payment-events"}, groupId = "payment-service-group")
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

            log.info("Received Kafka message on [order-events] in Payment Service: eventType={}, eventId={}", eventType, eventId);

            if ("InitiatePaymentCommand".equals(eventType)) {
                paymentService.handleInitiatePaymentCommand(eventId, correlationId, message);
            }
        } catch (Exception e) {
            log.error("Error consuming message from order-events in payment service: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
