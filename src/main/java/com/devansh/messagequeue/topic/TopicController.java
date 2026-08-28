package com.devansh.messagequeue.topic;

import com.devansh.messagequeue.message.ProduceMessageRequest;
import com.devansh.messagequeue.message.ProduceMessageResponse;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/topics")
public class TopicController {
    private final TopicService topicService;
    public TopicController(TopicService topicService) {
        this.topicService = topicService;
    }

    @PostMapping
    public void createTopic(@RequestBody CreateTopicRequest request) {
        topicService.createTopic(request.name(), request.partitions());
    }

    @PostMapping("/{topic}/messages")
    public ProduceMessageResponse produce(@PathVariable String topic, @RequestBody ProduceMessageRequest request) {
        return topicService.produce(topic, request.key(), request.value());
    }
}
