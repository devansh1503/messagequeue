package com.devansh.messagequeue.consumer.metrics;

import com.devansh.messagequeue.consumer.ConsumerGroup;
import com.devansh.messagequeue.consumer.ConsumerGroupService;
import com.devansh.messagequeue.topic.Topic;
import com.devansh.messagequeue.topic.TopicService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class LoadMetricsService {
    private final ConsumerGroupService consumerGroupService;
    private final TopicService topicService;
    public LoadMetricsService(ConsumerGroupService consumerGroupService, TopicService topicService) {
        this.consumerGroupService = consumerGroupService;
        this.topicService = topicService;
    }

    public ConsumerGroupLoad getGroupLoad(String groupName){
        ConsumerGroup group = consumerGroupService.getGroup(groupName);
        if(group == null){
            throw new IllegalArgumentException("Consumer group does not exist: "+groupName);
        }
        Topic topic = topicService.getTopic(group.getTopic());
        List<PartitionLoad> partitionLoads = new ArrayList<>();
        for(int partition=0; partition< topic.getPartitionCount(); partition++){
            long logEndOffset = topic.getPartition(partition).getNextOffset();
            long commitedOffset = consumerGroupService.getCommitedOffset(groupName, partition);
            long backlog = calculateBacklog(logEndOffset, commitedOffset);

            partitionLoads.add(new PartitionLoad(partition, logEndOffset, commitedOffset, backlog));
        }
        List<ConsumerLoad> consumerLoads = calculateConsumerLoads(group, partitionLoads);

        long totalBacklog = partitionLoads.stream().mapToLong(PartitionLoad::backlog).sum();

        return new ConsumerGroupLoad(groupName, group.getTopic(), partitionLoads, consumerLoads, totalBacklog);
    }

    private long calculateBacklog(long logEndOffset, long committedOffset){
        return Math.max(0, logEndOffset - committedOffset - 1);
    }

    private List<ConsumerLoad> calculateConsumerLoads(ConsumerGroup group, List<PartitionLoad> partitionLoads){
        List<ConsumerLoad> consumerLoads = new ArrayList<>();
        for(String consumer : group.getConsumers()){
            List<Integer> assignedPartitions = group.getAssignments()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().equals(consumer))
                    .map(Map.Entry::getKey)
                    .sorted()
                    .toList();

            long totalBacklog = partitionLoads.stream()
                    .filter(load -> assignedPartitions.contains(load.partitionId()))
                    .mapToLong(PartitionLoad::backlog)
                    .sum();

            consumerLoads.add(new ConsumerLoad(consumer, assignedPartitions, totalBacklog));
        }

        return consumerLoads;
    }
}
