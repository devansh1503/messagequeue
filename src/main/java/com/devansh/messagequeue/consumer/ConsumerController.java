package com.devansh.messagequeue.consumer;

import com.devansh.messagequeue.message.Message;
import com.devansh.messagequeue.topic.TopicService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/consume")
public class ConsumerController {
    private final TopicService topicService;
    public ConsumerController(TopicService topicService) {
        this.topicService = topicService;
    }

    @GetMapping("/{topic}/{partition}")
    public List<Message> consume(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestParam long offset,
            @RequestParam(defaultValue = "10") int limit)
    {
        return topicService.consume(topic, partition, offset, limit);
    }
}
