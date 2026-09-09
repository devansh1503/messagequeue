package com.devansh.messagequeue.consumer.metrics;

import java.util.List;

public record ConsumerLoad(
        String consumerId,
        List<Integer> partitions,
        long totalBacklog
) {
}
