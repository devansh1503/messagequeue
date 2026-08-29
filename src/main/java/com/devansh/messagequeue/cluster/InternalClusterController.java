package com.devansh.messagequeue.cluster;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/cluster")
public class InternalClusterController {
    private final BrokerHeartbeatService brokerHeartbeatService;
    public InternalClusterController(BrokerHeartbeatService brokerHeartbeatService) {
        this.brokerHeartbeatService = brokerHeartbeatService;
    }

    @PostMapping("/heartbeat/{brokerId}")
    public void heartbeat(@PathVariable int brokerId){
        brokerHeartbeatService.receiveHearbeat(brokerId);
    }
}
