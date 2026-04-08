package com.demo.mvc;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@CrossOrigin(origins = "*")
public class MetricsController {

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        List<Map<String, Object>> threads = Thread.getAllStackTraces().keySet().stream()
            .map(t -> {
                String name = t.getName();
                boolean isKafka = name.toLowerCase().contains("kafka")
                    || name.toLowerCase().contains("listener");
                return Map.<String, Object>of(
                    "name", name,
                    "state", t.getState().name(),
                    "isKafkaThread", isKafka
                );
            })
            .toList();

        long kafkaThreadCount = threads.stream()
            .filter(t -> (boolean) t.get("isKafkaThread"))
            .count();

        long blockedCount = threads.stream()
            .filter(t -> (boolean) t.get("isKafkaThread"))
            .filter(t -> {
                String state = (String) t.get("state");
                return state.equals("BLOCKED") || state.equals("TIMED_WAITING");
            })
            .count();

        return Map.of(
            "threadCount", threads.size(),
            "threads", threads,
            "kafkaThreadCount", kafkaThreadCount,
            "blockedCount", blockedCount,
            "messagesProcessed", BatchKafkaListener.messagesProcessed.get()
        );
    }
}
