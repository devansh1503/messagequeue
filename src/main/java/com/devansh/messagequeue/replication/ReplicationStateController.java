package com.devansh.messagequeue.replication;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/cluster/replication")
public class ReplicationStateController {
    private final ReplicationStateService replicationStateService;

    public ReplicationStateController(ReplicationStateService replicationStateService) {
        this.replicationStateService = replicationStateService;
    }

    @GetMapping("/{topic}/{partition}")
    public ReplicationStateResponse getState(@PathVariable String topic, @PathVariable int partition) {
        PartitionReplicationState state = replicationStateService.getReplicaState(topic, partition);

        return new ReplicationStateResponse(
                topic,
                partition,
                state.getLeaderOffset(),
                Map.copyOf(state.getReplicaOffsets()),
                replicationStateService.getInSyncReplicas(topic, partition)
        );
    }
}
