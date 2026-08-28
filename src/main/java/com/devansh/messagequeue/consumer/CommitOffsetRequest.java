package com.devansh.messagequeue.consumer;

public record CommitOffsetRequest(
        String topic,
        int partition,
        long offset
) {
}
