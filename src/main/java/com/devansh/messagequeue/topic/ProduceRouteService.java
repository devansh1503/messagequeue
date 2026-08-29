package com.devansh.messagequeue.topic;

import com.devansh.messagequeue.broker.BrokerIdentity;
import com.devansh.messagequeue.broker.BrokerNode;
import com.devansh.messagequeue.broker.PartitionMetaData;
import com.devansh.messagequeue.cluster.BrokerClient;
import com.devansh.messagequeue.cluster.ClusterMetadataService;
import com.devansh.messagequeue.message.AckMode;
import com.devansh.messagequeue.message.ProduceMessageRequest;
import com.devansh.messagequeue.message.ProduceMessageResponse;
import com.devansh.messagequeue.replication.LeaderProduceService;
import org.springframework.stereotype.Service;

@Service
public class ProduceRouteService {
    private final TopicService topicService;
    private final ClusterMetadataService clusterMetadataService;
    private final BrokerIdentity brokerIdentity;
    private final BrokerClient brokerClient;
    private final LeaderProduceService leaderProduceService;

    public ProduceRouteService(
            TopicService topicService,
            ClusterMetadataService clusterMetadataService,
            BrokerIdentity brokerIdentity,
            BrokerClient brokerClient,
            LeaderProduceService leaderProduceService
    ){
        this.topicService = topicService;
        this.clusterMetadataService = clusterMetadataService;
        this.brokerIdentity = brokerIdentity;
        this.brokerClient = brokerClient;
        this.leaderProduceService = leaderProduceService;
    }

    public ProduceMessageResponse produce(String topic, ProduceMessageRequest request, AckMode ackMode){
        int partition = topicService.selectPartition(topic, request.key());
        PartitionMetaData metaData =
                clusterMetadataService.getPartitionMetaData(topic, partition);

        if(metaData.leaderBrokerId() == brokerIdentity.getBrokerId()){
            return leaderProduceService.produce(topic, partition, request, ackMode);
        }

        BrokerNode leader = clusterMetadataService.getBroker(metaData.leaderBrokerId());
        return brokerClient.produce(
                leader,
                topic,
                partition,
                request,
                ackMode
        );
    }
}
