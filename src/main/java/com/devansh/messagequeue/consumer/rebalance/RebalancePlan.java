package com.devansh.messagequeue.consumer.rebalance;

import java.util.Map;

public record RebalancePlan(
        String groupName,
        String strategy,
        Map<Integer, String> currentAssignments,
        Map<Integer, String> proposedAssignments,
        int partitionsMoved
) {
}