package com.devansh.messagequeue.replication;

import java.util.Map;
import java.util.Set;

public record ReplicationStateResponse(
        String topic,
        int partition,
        long leaderOffset,
        Map<Integer, Long> replicaOffsets,
        Set<Integer> inSyncReplicas
) {
}
