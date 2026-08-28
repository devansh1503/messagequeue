package com.devansh.messagequeue.consumer;

public record JoinGroupRequest(
        String topic,
        String consumerId
) {
}
