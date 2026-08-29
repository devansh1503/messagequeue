package com.devansh.messagequeue.cluster;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/cluster")
public class ClusterHealthController {
    private final BrokerHeartbeatService brokerHeartbeatService;
    public ClusterHealthController(BrokerHeartbeatService brokerHeartbeatService) {
        this.brokerHeartbeatService = brokerHeartbeatService;
    }

    @GetMapping("/brokers")
    public Map<Integer, Boolean> brokers(){
        return brokerHeartbeatService.getBrokerStatuses();
    }
}
