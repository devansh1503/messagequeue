package com.devansh.messagequeue.topic;

import com.devansh.messagequeue.message.ProduceMessageRequest;
import com.devansh.messagequeue.message.ProduceMessageResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/topics")
@CrossOrigin(origins = "http://localhost:5173")
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

    @GetMapping
    public List<TopicInfo> getTopics(){
        return topicService.getTopics();
    }

    @GetMapping("/{topic}")
    public TopicInfo getTopic(
            @PathVariable String topic
    ){
        return topicService.getTopicInfo(topic);
    }
}
