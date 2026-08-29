package com.devansh.messagequeue.broker;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
public class BrokerIdentity {
    private final int brokerId;
    private final String host;
    private final int port;

    public BrokerIdentity(
            @Value("${broker.id}") int brokerId,
            @Value("${broker.host:localhost") String host,
            @Value("${server.port:8080}") int port
    ){
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
    }
}
