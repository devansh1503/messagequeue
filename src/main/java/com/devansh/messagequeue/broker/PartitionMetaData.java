package com.devansh.messagequeue.broker;

import java.util.List;

public record PartitionMetaData(
        String topic,
        int partition,
        int leaderBrokerId,
        List<Integer> replicaBrokerIds
) {
}
