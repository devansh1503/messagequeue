package com.devansh.messagequeue.cluster;

import com.devansh.messagequeue.message.Message;
import com.devansh.messagequeue.message.ProduceMessageRequest;
import com.devansh.messagequeue.message.ProduceMessageResponse;
import com.devansh.messagequeue.replication.ReplicationService;
import com.devansh.messagequeue.topic.TopicService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal/topics")
public class InternalBrokerController {
    private final TopicService topicService;
    private final ReplicationService replicationService;

    public InternalBrokerController(TopicService topicService, ReplicationService replicationService) {
        this.topicService = topicService;
        this.replicationService = replicationService;
    }

    @PostMapping("/{topic}/partitions/{partition}/messages")
    public ProduceMessageResponse produceInternal(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestBody ProduceMessageRequest messageRequest
    ){
        Message message = topicService.appendToPartition(
                topic,
                partition,
                messageRequest.key(),
                messageRequest.value()
        );

        replicationService.replicate(
                topic,
                partition,
                message
        );
        return new ProduceMessageResponse(
                topic,
                partition,
                message.offset()
        );
    }
}
