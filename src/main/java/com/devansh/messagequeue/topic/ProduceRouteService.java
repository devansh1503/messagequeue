package com.devansh.messagequeue.topic;

import com.devansh.messagequeue.broker.BrokerIdentity;
import com.devansh.messagequeue.broker.BrokerNode;
import com.devansh.messagequeue.broker.PartitionMetaData;
import com.devansh.messagequeue.cluster.BrokerClient;
import com.devansh.messagequeue.cluster.ClusterMetadataService;
import com.devansh.messagequeue.message.Message;
import com.devansh.messagequeue.message.ProduceMessageRequest;
import com.devansh.messagequeue.message.ProduceMessageResponse;
import com.devansh.messagequeue.replication.ReplicationService;
import org.springframework.stereotype.Service;

@Service
public class ProduceRouteService {
    private final TopicService topicService;
    private final ClusterMetadataService clusterMetadataService;
    private final BrokerIdentity brokerIdentity;
    private final BrokerClient brokerClient;
    private final ReplicationService replicationService;

    public ProduceRouteService(
            TopicService topicService,
            ClusterMetadataService clusterMetadataService,
            BrokerIdentity brokerIdentity,
            BrokerClient brokerClient,
            ReplicationService replicationService
    ){
        this.topicService = topicService;
        this.clusterMetadataService = clusterMetadataService;
        this.brokerIdentity = brokerIdentity;
        this.brokerClient = brokerClient;
        this.replicationService = replicationService;
    }

    public ProduceMessageResponse produce(String topic, ProduceMessageRequest request){
        int partition = topicService.selectPartition(topic, request.key());
        PartitionMetaData metaData =
                clusterMetadataService.getPartitionMetaData(topic, partition);

        if(metaData.leaderBrokerId() == brokerIdentity.getBrokerId()){
            Message message = topicService.appendToPartition(
                    topic,
                    partition,
                    request.key(),
                    request.value()
            );

            replicationService.replicate(
                    topic,
                    partition,
                    message
            );

            return topicService.produceToPartition(
                    topic,
                    partition,
                    request.key(),
                    request.value()
            );
        }

        BrokerNode leader = clusterMetadataService.getBroker(metaData.leaderBrokerId());
        return brokerClient.produce(
                leader,
                topic,
                partition,
                request
        );
    }
}
