package com.devansh.messagequeue.consumer.metrics;

import java.util.List;

public record ConsumerGroupLoad (
        String groupName,
        String topic,
        List<PartitionLoad> partitions,
        List<ConsumerLoad> consumers,
        long totalBacklog
){
}
