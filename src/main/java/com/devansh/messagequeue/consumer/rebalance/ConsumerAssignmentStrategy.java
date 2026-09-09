package com.devansh.messagequeue.consumer.rebalance;

import com.devansh.messagequeue.consumer.metrics.PartitionLoad;

import java.util.List;
import java.util.Map;

public interface ConsumerAssignmentStrategy {
    Map<Integer, String> assign(List<String> consumers, List<PartitionLoad> partitionLoads);
}
