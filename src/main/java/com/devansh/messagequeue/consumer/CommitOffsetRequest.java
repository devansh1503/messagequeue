package com.devansh.messagequeue.consumer;

public record CommitOffsetRequest(
        int partition,
        long offset
) {
}
