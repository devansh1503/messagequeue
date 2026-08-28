package com.devansh.messagequeue.message;

public record Message(
        String key,
        String value,
        String topic,
        int partition,
        long offset,
        long timestamp,
        int size,
        int crc
) {
}
