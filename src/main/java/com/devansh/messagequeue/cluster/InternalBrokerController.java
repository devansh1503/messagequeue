package com.devansh.messagequeue.cluster;

import com.devansh.messagequeue.message.ProduceMessageRequest;
import com.devansh.messagequeue.message.ProduceMessageResponse;
import com.devansh.messagequeue.topic.TopicService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal/topics")
public class InternalBrokerController {
    private final TopicService topicService;
    public InternalBrokerController(TopicService topicService) {
        this.topicService = topicService;
    }

    @PostMapping("/{topic}/partitions/{partition}/messages")
    public ProduceMessageResponse produceInternal(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestBody ProduceMessageRequest messageRequest
    ){
        return topicService.produceToPartition(
                topic,
                partition,
                messageRequest.key(),
                messageRequest.value()
        );
    }
}
