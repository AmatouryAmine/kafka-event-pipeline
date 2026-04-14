package com.pipeline.monitoring;

import com.pipeline.consumer.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/api/monitor")
@RequiredArgsConstructor
public class KafkaMonitorController {

    private final ProcessedEventRepository processedEventRepository;

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        long processedCount = processedEventRepository.count();

        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "processedEvents", processedCount,
                "timestamp", Instant.now().toString()
        ));
    }
}
