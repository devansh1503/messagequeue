package com.devansh.messagequeue.broker;

import com.devansh.messagequeue.consumer.ConsumerGroupService;
import com.devansh.messagequeue.topic.TopicService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/broker")
@CrossOrigin(origins = "http://localhost:5173")
public class BrokerController {

    private final TopicService topicService;
    private final ConsumerGroupService consumerGroupService;
    private final BrokerIdentity brokerIdentity;

    public BrokerController(
            TopicService topicService,
            ConsumerGroupService consumerGroupService,
            BrokerIdentity brokerIdentity
    ){
        this.topicService = topicService;
        this.consumerGroupService =
                consumerGroupService;
        this.brokerIdentity = brokerIdentity;
    }

    @GetMapping
    public BrokerInfo getBrokerInfo(){

        return new BrokerInfo(
                brokerIdentity.getBrokerId(),
                brokerIdentity.getHost(),
                brokerIdentity.getPort(),
                "RUNNING",
                topicService.getTopicCount(),
                topicService.getTotalPartitionCount(),
                consumerGroupService.getGroupCount(),
                consumerGroupService
                        .getActiveConsumerCount(),
                topicService.getTotalMessageCount()
        );
    }
}