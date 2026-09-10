package com.distributedsystem.inventory.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    @SuppressWarnings("null")
    public CommonErrorHandler errorHandler(KafkaOperations<Object, Object> kafkaOperations) {
        if (kafkaOperations == null) {
            return new DefaultErrorHandler(new FixedBackOff(1000L, 3L));
        }

        // DeadLetterPublishingRecoverer routes failed / unparseable messages to <original-topic>.DLT
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaOperations,
            (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition())
        );

        // 3 retry attempts with 1000ms backoff before routing to DLT
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
    }
}
