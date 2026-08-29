package com.devansh.messagequeue.cluster;

import com.devansh.messagequeue.broker.BrokerNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class ClusterConfig {
    private final List<BrokerNode> brokers;

    public ClusterConfig(
            @Value("${cluster.brokers}") String brokerConfig
    ){
        this.brokers = Arrays.stream(brokerConfig.split(","))
                .map(this::parseBroker)
                .toList();
    }

    private BrokerNode parseBroker(String brokerConfig){
        String[] parts = brokerConfig.split(":");
        return new BrokerNode(
                Integer.parseInt(parts[0]),
                parts[1],
                Integer.parseInt(parts[2])
        );
    }

    public List<BrokerNode> getBrokers() {
        return brokers;
    }
}
