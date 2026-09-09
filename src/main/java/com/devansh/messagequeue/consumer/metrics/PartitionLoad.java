package com.devansh.messagequeue.consumer.metrics;

public record PartitionLoad(
        int partitionId,
        long logEndOffset,
        long committedOffset,
        long backlog
) {
}
