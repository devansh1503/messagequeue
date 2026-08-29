package com.devansh.messagequeue.consumer;

import java.util.List;

public record ConsumerInfo(
        String id,
        String status,
        long lastHeartbeat,
        List<Integer> partitions
) {}
