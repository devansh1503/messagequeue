package com.devansh.messagequeue.consumer;

import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class ConsumerGroup {
    private final String name;
    private final String topic;
    private final Set<String> consumers =
            ConcurrentHashMap.newKeySet();
    private final Map<Integer, String> assignments =
            new ConcurrentHashMap<>();
    private final Map<Integer, Long> commitedOffsets =
            new ConcurrentHashMap<>();
    private final Map<String, Long> lastHeartbeats =
            new ConcurrentHashMap<>();

    public ConsumerGroup(String name, String topic) {
        this.name = name;
        this.topic = topic;
    }
}
