package com.devansh.messagequeue.consumer.rebalance;

import com.devansh.messagequeue.consumer.metrics.PartitionLoad;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class RoundRobinAssignmentStrategy implements ConsumerAssignmentStrategy {
    @Override
    public Map<Integer, String> assign(List<String>consumers, List<PartitionLoad> partitionLoads){
        if(consumers == null || consumers.isEmpty()){
            return Collections.emptyMap();
        }
        List<String> sortedConsumers = new ArrayList<>(consumers);
        Collections.sort(sortedConsumers);

        List<PartitionLoad> sortedPartitions = partitionLoads.stream()
                .sorted(
                        Comparator.comparingInt(
                                PartitionLoad::partitionId
                        )
                ).toList();
        Map<Integer, String> assignments = new LinkedHashMap<>();
        for(int i=0; i<sortedPartitions.size(); i++){
            PartitionLoad partition = sortedPartitions.get(i);
            String consumer = sortedConsumers.get(i % sortedConsumers.size());

            assignments.put(partition.partitionId(), consumer);
        }

        return assignments;
    }
}
