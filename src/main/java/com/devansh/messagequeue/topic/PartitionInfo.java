package com.devansh.messagequeue.topic;

public record PartitionInfo(
        int id,
        long nextOffset
) {}