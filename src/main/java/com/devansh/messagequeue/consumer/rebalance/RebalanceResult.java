package com.devansh.messagequeue.consumer.rebalance;

import java.util.Map;

public record RebalanceResult (
        String groupName,
        String strategy,
        Map<Integer, String> previousAssignments,
        Map<Integer, String> newAssignments,
        int partitionsMoved
){
}
