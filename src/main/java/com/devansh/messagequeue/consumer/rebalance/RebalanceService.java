package com.devansh.messagequeue.consumer.rebalance;

import com.devansh.messagequeue.consumer.ConsumerGroup;
import com.devansh.messagequeue.consumer.ConsumerGroupService;
import com.devansh.messagequeue.consumer.metrics.ConsumerGroupLoad;
import com.devansh.messagequeue.consumer.metrics.LoadMetricsService;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class RebalanceService {
    private final ConsumerGroupService consumerGroupService;
    private final LoadMetricsService loadMetricsService;
    private final RoundRobinAssignmentStrategy roundRobinAssignmentStrategy;
    private final LoadAwareAssignmentStrategy loadAwareAssignmentStrategy;

    public RebalanceService(
            ConsumerGroupService consumerGroupService,
            LoadMetricsService loadMetricsService,
            RoundRobinAssignmentStrategy roundRobinAssignmentStrategy,
            LoadAwareAssignmentStrategy loadAwareAssignmentStrategy
    ){
        this.consumerGroupService = consumerGroupService;
        this.loadMetricsService = loadMetricsService;
        this.roundRobinAssignmentStrategy = roundRobinAssignmentStrategy;
        this.loadAwareAssignmentStrategy = loadAwareAssignmentStrategy;
    }

    public RebalanceResult rebalance(String groupName, String strategyName){
        ConsumerGroup group = consumerGroupService.getGroup(groupName);
        if(group == null) {
            throw new IllegalArgumentException("groupName cannot be null");
        }
        if(group.getConsumers().isEmpty()){
            throw new IllegalArgumentException("consumers cannot be empty");
        }
        ConsumerGroupLoad groupLoad = loadMetricsService.getGroupLoad(groupName);
        List<String> consumers = new ArrayList<>(group.getConsumers());
        ConsumerAssignmentStrategy strategy = resolveStrategy(strategyName);
        Map<Integer, String> previousAssignments = new TreeMap<>(
                group.getAssignments()
        );
        Map<Integer, String> newAssignments = strategy.assign(
                consumers,
                groupLoad.partitions()
        );
        int parititonsMoved = calculateMovedParitions(previousAssignments, newAssignments);
        group.getAssignments().clear();
        group.getAssignments().putAll(newAssignments);

        return new RebalanceResult(
                groupName,
                strategyName.toUpperCase(),
                previousAssignments,
                new TreeMap<>(newAssignments),
                parititonsMoved
        );
    }

    private ConsumerAssignmentStrategy resolveStrategy(String strategyName){
        if(strategyName == null){
            throw new IllegalArgumentException("strategyName cannot be null");
        }

        return switch (strategyName.toUpperCase()){
            case "ROUND_ROBIN" -> roundRobinAssignmentStrategy;
            case "LOAD_AWARE" -> loadAwareAssignmentStrategy;
            default -> throw new IllegalArgumentException("Unknown strategy: " + strategyName);
        };
    }

    private int calculateMovedParitions(Map<Integer, String> previousAssignments, Map<Integer, String> newAssignments){
        int moved = 0;
        for(Map.Entry<Integer, String> entry : newAssignments.entrySet()){
            String oldConsumer = previousAssignments.get(entry.getKey());
            if(!Objects.equals(oldConsumer, entry.getValue())){
                moved++;
            }
        }
        return moved;
    }

}
