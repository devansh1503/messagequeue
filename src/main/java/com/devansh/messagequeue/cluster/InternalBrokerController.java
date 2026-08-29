package com.devansh.messagequeue.cluster;

import com.devansh.messagequeue.message.AckMode;
import com.devansh.messagequeue.message.Message;
import com.devansh.messagequeue.message.ProduceMessageRequest;
import com.devansh.messagequeue.message.ProduceMessageResponse;
import com.devansh.messagequeue.replication.LeaderProduceService;
import com.devansh.messagequeue.replication.ReplicationService;
import com.devansh.messagequeue.topic.TopicService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal/topics")
public class InternalBrokerController {
    private final LeaderProduceService leaderProduceService;

    public InternalBrokerController(LeaderProduceService leaderProduceService) {
        this.leaderProduceService = leaderProduceService;
    }

    @PostMapping("/{topic}/partitions/{partition}/messages")
    public ProduceMessageResponse produceInternal(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestParam(defaultValue = "all") String acks,
            @RequestBody ProduceMessageRequest messageRequest
    ){
        AckMode ackMode = AckMode.from(acks);

        return leaderProduceService.produce(
                topic,
                partition,
                messageRequest,
                ackMode
        );
    }
}
