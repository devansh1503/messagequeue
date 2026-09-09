package com.devansh.messagequeue.consumer.rebalance;

import com.devansh.messagequeue.consumer.metrics.PartitionLoad;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class LoadAwareAssignmentStrategy implements ConsumerAssignmentStrategy{
    @Override
    public Map<Integer, String> assign(List<String> consumers, List<PartitionLoad> partitionLoads) {
        if(consumers == null || consumers.isEmpty()){
            return Collections.emptyMap();
        }
        List<String> sortedConsumers = new ArrayList<>(consumers);
        Collections.sort(sortedConsumers);

        List<PartitionLoad> sortedPartitions = partitionLoads.stream()
                .sorted(
                        Comparator.comparingLong(
                                PartitionLoad::backlog
                        ).reversed()
                                .thenComparingInt(
                                        PartitionLoad::partitionId
                                )
                ).toList();
        Map<Integer, String> assignments = new LinkedHashMap<>();
        Map<String, Long> consumerLoad = new HashMap<>();

        for(String consumer : sortedConsumers){
            consumerLoad.put(consumer, 0L);
        }

        for(PartitionLoad partitionLoad : sortedPartitions){
            String leastLoadedConsumer = sortedConsumers.stream()
                    .min(
                            Comparator.comparingLong((String consumer) -> consumerLoad.get(consumer))
                                    .thenComparing((String consumer) -> consumer)
                    ).orElseThrow();

            assignments.put(
                    partitionLoad.partitionId(),
                    leastLoadedConsumer
            );

            consumerLoad.put(
                    leastLoadedConsumer,
                    consumerLoad.get(
                            leastLoadedConsumer
                    ) + partitionLoad.backlog()
            );
        }
        return assignments;
    }
}
