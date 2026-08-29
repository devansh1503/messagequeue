package com.devansh.messagequeue.consumer;

public record ConsumerGroupSummary(
        String name,
        String topic,
        int consumerCount
) {
}
