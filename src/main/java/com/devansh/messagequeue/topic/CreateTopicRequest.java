package com.devansh.messagequeue.topic;

public record CreateTopicRequest(
        String name,
        int partitions
) {
}
