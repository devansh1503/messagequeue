package com.devansh.messagequeue.broker;

public record BrokerInfo(
        String status,
        int topics,
        int partitions,
        int consumerGroups,
        int activeConsumers,
        long StoredMessages
) {
}
