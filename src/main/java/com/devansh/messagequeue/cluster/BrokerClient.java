package com.devansh.messagequeue.cluster;

import com.devansh.messagequeue.broker.BrokerNode;
import com.devansh.messagequeue.message.ProduceMessageRequest;
import com.devansh.messagequeue.message.ProduceMessageResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class BrokerClient {
    private final RestClient restClient;
    public BrokerClient() {
        this.restClient = RestClient.create();
    }

    public ProduceMessageResponse produce(BrokerNode broker, String topic, int partition, ProduceMessageRequest request){
        String url = "http://"+broker.host()+":"+broker.port()+"/internal/topics/"+topic+"/partitions/"+partition+"/messages";
        return restClient.post().uri(url).body(request).retrieve().body(ProduceMessageResponse.class);
    }
}
