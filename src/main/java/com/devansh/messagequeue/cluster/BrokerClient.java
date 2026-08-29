package com.devansh.messagequeue.cluster;

import com.devansh.messagequeue.broker.BrokerNode;
import com.devansh.messagequeue.message.AckMode;
import com.devansh.messagequeue.message.Message;
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

    public ProduceMessageResponse produce(BrokerNode broker, String topic, int partition, ProduceMessageRequest request, AckMode ackMode) {
        String acks = switch (ackMode) {
            case ZERO -> "0";
            case ONE -> "1";
            case ALL -> "all";
        };
        String url = "http://"+broker.host()+":"+broker.port()+"/internal/topics/"+topic+"/partitions/"+partition+"/messages?acks="+acks;
        return restClient.post().uri(url).body(request).retrieve().body(ProduceMessageResponse.class);
    }

    public void replicate(BrokerNode broker, String topic, int partition, Message message){
        String url = "http://"+broker.host()+":"+broker.port()+"/internal/replication/"+topic+"/"+partition;
        restClient.post().uri(url).body(message).retrieve().toBodilessEntity();
    }

    public void sendHeartbeat(BrokerNode broker, int senderBrokerId){
        String url = "http://"+broker.host()+":"+broker.port()+"/internal/cluster/heartbeat/"+senderBrokerId;
        restClient.post().uri(url).retrieve().toBodilessEntity();
    }
}
