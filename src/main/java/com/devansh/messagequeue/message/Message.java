package com.devansh.messagequeue.message;

public record Message(
        byte[]key,
        byte[]value,
        String topic,
        int partition,
        long offset,
        long timestamp,
        int size,
        int crc
) {
}
