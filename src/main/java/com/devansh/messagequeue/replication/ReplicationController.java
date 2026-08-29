package com.devansh.messagequeue.replication;

import com.devansh.messagequeue.message.Message;
import com.devansh.messagequeue.topic.TopicService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal/replication")
public class ReplicationController {
    private final TopicService topicService;
    public ReplicationController(TopicService topicService) {
        this.topicService = topicService;
    }

    @PostMapping("/{topic}/{partition}")
    public void replicate(@PathVariable String topic, @PathVariable int partition, @RequestBody Message message) {
        topicService.appendReplica(topic, partition, message);
    }
}
