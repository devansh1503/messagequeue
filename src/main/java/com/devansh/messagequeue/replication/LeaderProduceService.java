package com.devansh.messagequeue.replication;

import com.devansh.messagequeue.message.AckMode;
import com.devansh.messagequeue.message.Message;
import com.devansh.messagequeue.message.ProduceMessageRequest;
import com.devansh.messagequeue.message.ProduceMessageResponse;
import com.devansh.messagequeue.topic.TopicService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class LeaderProduceService {
    private final TopicService topicService;
    private final ReplicationService replicationService;
    private final ReplicationStateService replicationStateService;
    private final TaskExecutor taskExecutor;

    public LeaderProduceService(
            TopicService topicService,
            ReplicationService replicationService,
            ReplicationStateService replicationStateService,
            @Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor
    ){
        this.topicService = topicService;
        this.replicationService = replicationService;
        this.replicationStateService = replicationStateService;
        this.taskExecutor = taskExecutor;
    }

    public ProduceMessageResponse produce(
            String topic,
            int partition,
            ProduceMessageRequest request,
            AckMode ackMode
    ){
        return switch (ackMode) {
            case ZERO -> produceWithNoAck(topic, partition, request);
            case ONE -> produceWithLeaderAck(topic, partition, request);
            case ALL -> produceWithAllAck(topic, partition, request);
        };
    }

    public ProduceMessageResponse produceWithNoAck(
            String topic,
            int partition,
            ProduceMessageRequest request
    ){
        taskExecutor.execute(() -> {
            Message message = appendOnLeader(topic, partition, request);
            replicationService.replicate(topic, partition, message);
        });

        return new ProduceMessageResponse(topic, partition, -1);
    }

    public ProduceMessageResponse produceWithLeaderAck(
            String topic,
            int partition,
            ProduceMessageRequest request
    ){
        Message message = appendOnLeader(topic, partition, request);
        taskExecutor.execute(() -> replicationService.replicate(topic, partition, message));

        return new ProduceMessageResponse(topic, partition, message.offset());
    }

    public ProduceMessageResponse produceWithAllAck(
            String topic,
            int partition,
            ProduceMessageRequest request
    ){
        Message message = appendOnLeader(topic, partition, request);
        replicationService.replicate(topic, partition, message);

        if(!replicationStateService.areAllReplicasInSync(topic, partition)){
            throw new IllegalStateException("acks=all failed:All replicas are not in sync");
        }

        return new ProduceMessageResponse(topic, partition, message.offset());
    }

    private Message appendOnLeader(String topic, int partition, ProduceMessageRequest request){
        Message message = topicService.appendToPartition(topic, partition, request.key(), request.value());
        replicationStateService.recordLeaderOffset(topic, partition, message.offset());
        return message;
    }
}
