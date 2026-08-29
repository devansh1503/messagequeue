package com.devansh.messagequeue.replication;

import com.devansh.messagequeue.broker.PartitionMetaData;
import com.devansh.messagequeue.cluster.ClusterMetadataService;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ReplicationStateService {
    private final ConcurrentHashMap<String, PartitionReplicationState> states = new ConcurrentHashMap<>();
    private final ClusterMetadataService clusterMetadataService;
    public ReplicationStateService(ClusterMetadataService clusterMetadataService) {
        this.clusterMetadataService = clusterMetadataService;
    }

    private String key(String topic, int partition){
        return topic+":"+partition;
    }

    private PartitionReplicationState getState(String topic, int partition){
        return states.computeIfAbsent(
                key(topic, partition),
                ignored -> new PartitionReplicationState()
        );
    }

    public void recordLeaderOffset(String topic, int partition, long offset){
        getState(topic, partition).setLeaderOffset(offset);
    }

    public void recordReplicaOffset(String topic, int partition, int brokerId, long offset){
        getState(topic, partition).updateReplicaOffset(brokerId, offset);
    }

    public long getLeaderOffset(String topic, int partition){
        return getState(topic, partition).getLeaderOffset();
    }
    public PartitionReplicationState getReplicaState(String topic, int partition){
        return getState(topic, partition);
    }

    public Set<Integer> getInSyncReplicas(String topic, int partition){
        PartitionReplicationState state = getState(topic, partition);
        PartitionMetaData metaData = clusterMetadataService.getPartitionMetaData(topic, partition);
        Set<Integer> isr = new HashSet<>();

        long leaderOffset = state.getLeaderOffset();

        isr.add(metaData.leaderBrokerId());

        for(Integer brokerId : metaData.replicaBrokerIds()){
            if(brokerId == metaData.leaderBrokerId()){
                continue;
            }
            Long replicaOffset = state.getReplicaOffsets().get(brokerId);
            if(replicaOffset != null && replicaOffset == leaderOffset){
                isr.add(brokerId);
            }
        }
        return isr;
    }

    public boolean areAllReplicasInSync(String topic, int partition){
        PartitionMetaData metaData = clusterMetadataService.getPartitionMetaData(topic, partition);
        Set<Integer> isr = getInSyncReplicas(topic, partition);
        return isr.containsAll(metaData.replicaBrokerIds());
    }
}
