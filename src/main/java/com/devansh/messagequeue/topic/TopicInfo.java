package com.devansh.messagequeue.topic;

import java.util.List;

public record TopicInfo(
        String name,
        int partitionCount,
        List<PartitionInfo> partitions
) {
}
