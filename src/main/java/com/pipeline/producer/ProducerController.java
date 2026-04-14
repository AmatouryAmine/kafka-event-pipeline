package com.pipeline.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class ProducerController {

    private final OrderProducer orderProducer;

    @PostMapping
    public ResponseEntity<Map<String, String>> publishOrder(@RequestBody OrderRequest request) {
        UUID orderId = UUID.randomUUID();

        OrderEvent event = OrderEvent.builder()
                .orderId(orderId)
                .customerId(request.getCustomerId())
                .product(request.getProduct())
                .amount(request.getAmount())
                .timestamp(Instant.now())
                .build();

        orderProducer.sendOrder(event);

        log.info("Accepted order publish request: orderId={}", orderId);
        return ResponseEntity.accepted().body(Map.of(
                "orderId", orderId.toString(),
                "status", "published"
        ));
    }
}
