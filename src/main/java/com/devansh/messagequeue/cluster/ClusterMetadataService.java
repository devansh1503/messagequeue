package com.devansh.messagequeue.cluster;

import com.devansh.messagequeue.broker.BrokerNode;
import com.devansh.messagequeue.broker.PartitionMetaData;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ClusterMetadataService {
    private final List<BrokerNode> brokers;
    private final BrokerHeartbeatService brokerHeartbeatService;
    private final Map<String, Integer> leaderOverrides = new ConcurrentHashMap<>();

    public ClusterMetadataService(ClusterConfig clusterConfig, BrokerHeartbeatService brokerHeartbeatService) {
        this.brokers = clusterConfig.getBrokers();
        this.brokerHeartbeatService = brokerHeartbeatService;
    }

    public PartitionMetaData getPartitionMetaData(
            String topic,
            int partition
    ) {
        int brokerCount = brokers.size();
        BrokerNode preferredLeader = brokers.get(partition % brokerCount);
        BrokerNode follower = brokers.get((partition + 1) % brokerCount);

        List<Integer> replicas = List.of(preferredLeader.id(), follower.id());

        int leaderBrokerId = resolveLeader(topic, partition, preferredLeader.id(), replicas);

        return new PartitionMetaData(topic, partition, leaderBrokerId, replicas);
    }

    private int resolveLeader(String topic, int partition, int preferredLeader, List<Integer>replicas){
        String key = topic+":"+partition;
        Integer currentOverride = leaderOverrides.get(key);

        if(currentOverride != null && brokerHeartbeatService.isBrokerAlive(currentOverride)){
            return currentOverride;
        }

        if(currentOverride == null && brokerHeartbeatService.isBrokerAlive(preferredLeader)){
            return preferredLeader;
        }

        for(Integer replica : replicas){
            if(brokerHeartbeatService.isBrokerAlive(replica)){
                leaderOverrides.put(key, replica);
            }
            return replica;
        }

        throw new IllegalStateException("No Alive Replica available");
    }

    public BrokerNode getBroker(int brokerId){
        return brokers.stream().filter(b -> b.id() == brokerId).findFirst().orElse(null);
    }
}
