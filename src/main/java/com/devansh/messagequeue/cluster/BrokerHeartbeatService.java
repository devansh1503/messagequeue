package com.devansh.messagequeue.cluster;

import com.devansh.messagequeue.broker.BrokerIdentity;
import com.devansh.messagequeue.broker.BrokerNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class BrokerHeartbeatService {
    private static final long HEARTBEAT_TIMEOUT_MS = 6000;
    private final Map<Integer, Long> lastHeartbeats = new ConcurrentHashMap<>();
    private final List<BrokerNode> brokers;
    private final BrokerIdentity brokerIdentity;
    private final BrokerClient brokerClient;
    private final TaskExecutor taskExecutor;

    public BrokerHeartbeatService(
            ClusterConfig clusterConfig,
            BrokerIdentity brokerIdentity,
            BrokerClient brokerClient,
            @Qualifier("applicationTaskExecutor")TaskExecutor taskExecutor
            ) {
        this.brokers = clusterConfig.getBrokers();
        this.brokerIdentity = brokerIdentity;
        this.brokerClient = brokerClient;
        this.taskExecutor = taskExecutor;
        long now = System.currentTimeMillis();

        for(BrokerNode broker : brokers) {
            lastHeartbeats.put(broker.id(), now);
        }
    }

    public void receiveHearbeat(int brokerId){
        lastHeartbeats.put(brokerId, System.currentTimeMillis());
    }

    public boolean isBrokerAlive(int brokerId){
        //A running broker knows that it itself is alive duhh!
        if(brokerId == brokerIdentity.getBrokerId()) {
            return true;
        }
        Long lastHeartbeat = lastHeartbeats.get(brokerId);

        if(lastHeartbeat == null) {
            return false;
        }

        return System.currentTimeMillis() - lastHeartbeat <= HEARTBEAT_TIMEOUT_MS;
    }

    @Scheduled(fixedRate = 2000)
    public void sendHeartbeats(){
        for(BrokerNode broker : brokers) {
            if(broker.id() == brokerIdentity.getBrokerId()) {
                continue;
            }
            taskExecutor.execute(() -> {
                try {
                    brokerClient.sendHeartbeat(broker, brokerIdentity.getBrokerId());
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            });
        }
    }

    public Map<Integer, Boolean> getBrokerStatuses(){
        Map<Integer, Boolean> brokerStatuses = new ConcurrentHashMap<>();
        for(BrokerNode broker : brokers) {
            brokerStatuses.put(broker.id(), isBrokerAlive(broker.id()));
        }
        return brokerStatuses;
    }
}
