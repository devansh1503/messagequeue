package com.devansh.messagequeue.replication;

import com.devansh.messagequeue.broker.BrokerIdentity;
import com.devansh.messagequeue.broker.BrokerNode;
import com.devansh.messagequeue.broker.PartitionMetaData;
import com.devansh.messagequeue.cluster.BrokerClient;
import com.devansh.messagequeue.cluster.ClusterMetadataService;
import com.devansh.messagequeue.message.Message;
import org.springframework.stereotype.Service;

@Service
public class ReplicationService {
    private final ClusterMetadataService clusterMetadataService;
    private final BrokerClient brokerClient;
    private final BrokerIdentity brokerIdentity;
    private final ReplicationStateService replicationStateService;

    public ReplicationService(
            ClusterMetadataService clusterMetadataService,
            BrokerClient brokerClient,
            BrokerIdentity brokerIdentity,
            ReplicationStateService replicationStateService
    ) {
        this.clusterMetadataService = clusterMetadataService;
        this.brokerClient = brokerClient;
        this.brokerIdentity = brokerIdentity;
        this.replicationStateService = replicationStateService;
    }

    public void replicate(String topic, int partition, Message message){
        PartitionMetaData metaData = clusterMetadataService.getPartitionMetaData(topic, partition);
        for(Integer brokerId : metaData.replicaBrokerIds()){
            if(brokerId == brokerIdentity.getBrokerId()){
                continue;
            }

            BrokerNode replica = clusterMetadataService.getBroker(brokerId);
            try {
                brokerClient.replicate(replica, topic, partition, message);
                replicationStateService.recordReplicaOffset(topic, partition, brokerId, message.offset());
            }catch (Exception e){
                System.out.println("Replication Failed For "+topic+":"+partition+" to broker "+brokerId);
            }
        }
    }
}
