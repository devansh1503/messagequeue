package com.devansh.messagequeue.consumer;

import java.util.List;
import java.util.Map;

public record ConsumerGroupInfo(
        String name,
        String topic,
        String status,
        List<ConsumerInfo> consumers,
        Map<Integer, Long> commitedOffsets
) {
}
