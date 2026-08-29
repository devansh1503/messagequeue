package com.devansh.messagequeue.cluster;

import com.devansh.messagequeue.broker.BrokerNode;
import com.devansh.messagequeue.broker.PartitionMetaData;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ClusterMetadataService {
    private final List<BrokerNode> brokers;

    public ClusterMetadataService(ClusterConfig clusterConfig){
        this.brokers = clusterConfig.getBrokers();
    }

    public PartitionMetaData getPartitionMetaData(
            String topic,
            int partition
    ) {
        int brokerCount = brokers.size();
        BrokerNode leader = brokers.get(partition % brokerCount);
        BrokerNode follower = brokers.get((partition + 1) % brokerCount);

        return new PartitionMetaData(topic, partition, leader.id(), List.of(leader.id(), follower.id()));
    }
}
