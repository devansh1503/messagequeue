package com.devansh.messagequeue.consumer.rebalance;

import com.devansh.messagequeue.consumer.metrics.PartitionLoad;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ImbalanceCalculator {

    public double calculate(List<String> consumers, List<PartitionLoad> partitionLoads, Map<Integer, String> assignments) {
        if (consumers == null || consumers.isEmpty()) {
            return 1.0;
        }
        Map<Integer, Long> partitionBacklogs = new HashMap<>();

        for (PartitionLoad partitionLoad : partitionLoads) {
            partitionBacklogs.put(
                    partitionLoad.partitionId(),
                    partitionLoad.backlog()
            );
        }

        Map<String, Long> consumerLoads = new HashMap<>();

        for (String consumer : consumers) {
            consumerLoads.put(consumer, 0L);
        }

        for (Map.Entry<Integer, String> assignment : assignments.entrySet()) {
            long partitionLoad = partitionBacklogs.getOrDefault(assignment.getKey(), 0L);

            String consumer = assignment.getValue();

            consumerLoads.computeIfPresent(consumer, (key, currentLoad) -> currentLoad + partitionLoad);
        }

        long totalLoad = consumerLoads.values()
                        .stream()
                        .mapToLong(Long::longValue)
                        .sum();

        if (totalLoad == 0) {
            return 1.0;
        }

        double averageLoad =
                (double) totalLoad
                        / consumers.size();

        long maxLoad =
                consumerLoads.values()
                        .stream()
                        .mapToLong(Long::longValue)
                        .max()
                        .orElse(0);

        return maxLoad / averageLoad;
    }
}