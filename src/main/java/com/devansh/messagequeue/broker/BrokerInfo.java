package com.devansh.messagequeue.broker;

public record BrokerInfo(
        int brokerId,
        String host,
        int port,
        String status,
        int topics,
        int partitions,
        int consumerGroups,
        int activeConsumers,
        long StoredMessages
) {
}
