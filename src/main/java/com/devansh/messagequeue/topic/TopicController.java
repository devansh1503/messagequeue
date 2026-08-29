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
    private final ProduceRouteService produceRouteService;
    public TopicController(TopicService topicService, ProduceRouteService produceRouteService) {
        this.topicService = topicService;
        this.produceRouteService = produceRouteService;
    }

    @PostMapping
    public void createTopic(@RequestBody CreateTopicRequest request) {
        topicService.createTopic(request.name(), request.partitions());
    }

    @PostMapping("/{topic}/messages")
    public ProduceMessageResponse produce(@PathVariable String topic, @RequestBody ProduceMessageRequest request) {
        return produceRouteService.produce(topic, request);
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
