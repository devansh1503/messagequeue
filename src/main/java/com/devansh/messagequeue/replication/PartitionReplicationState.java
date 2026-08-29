package com.devansh.messagequeue.replication;

import lombok.Getter;
import lombok.Setter;
import org.springframework.web.service.annotation.GetExchange;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class PartitionReplicationState {
    private volatile long leaderOffset = -1;
    private final Map<Integer, Long> replicaOffsets = new ConcurrentHashMap<>();

    public void updateReplicaOffset(int partitionId, long offset) {
        replicaOffsets.put(partitionId, offset);
    }
}
