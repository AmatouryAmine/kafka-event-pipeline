package com.pipeline.consumer;

import com.pipeline.producer.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderConsumer {

    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(topics = "orders", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void consume(OrderEvent event,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                        @Header(KafkaHeaders.OFFSET) Long offset) {

        String orderId = event.getOrderId().toString();
        log.info("Received order event: orderId={}, partition={}, offset={}", orderId, partition, offset);

        if (processedEventRepository.existsByOrderId(orderId)) {
            log.warn("Duplicate event detected — skipping: orderId={}", orderId);
            return;
        }

        processOrder(event);

        processedEventRepository.save(ProcessedEvent.builder()
                .orderId(orderId)
                .processedAt(Instant.now())
                .build());

        log.info("Order event processed and recorded: orderId={}", orderId);
    }

    private void processOrder(OrderEvent event) {
        // Core business logic placeholder
        // In a real system: persist to orders table, trigger downstream services, etc.
        log.info("Processing order: customerId={}, product={}, amount={}",
                event.getCustomerId(), event.getProduct(), event.getAmount());

        // Simulate occasional failures for DLT demonstration
        // To test DLT: send an order with product="FAIL"
        if ("FAIL".equalsIgnoreCase(event.getProduct())) {
            throw new RuntimeException("Simulated processing failure for orderId=" + event.getOrderId());
        }
    }
}
