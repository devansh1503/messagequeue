package com.devansh.messagequeue.broker;

public record BrokerNode(
        int id,
        String host,
        int port
) {
}
