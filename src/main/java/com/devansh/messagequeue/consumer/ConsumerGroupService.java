package com.devansh.messagequeue.consumer;

import com.devansh.messagequeue.message.Message;
import com.devansh.messagequeue.topic.Topic;
import com.devansh.messagequeue.topic.TopicService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerGroupService {
    private static final long HEARTBEAT_TIMEOUT_MS = 15_000;
    private final ConcurrentHashMap<String, ConsumerGroup> groups =
            new ConcurrentHashMap<>();
    private final TopicService topicService;

    public ConsumerGroupService(TopicService topicService) {
        this.topicService = topicService;
    }

    public synchronized ConsumerGroup join(
            String groupName,
            String topicName,
            String consumerId
    ){
        Topic topic = topicService.getTopic(topicName);
        ConsumerGroup group = groups.computeIfAbsent(
                groupName,
                name -> new ConsumerGroup(name, topicName)
        );

        group.getConsumers().add(consumerId);

        group.getLastHeartbeats().put(consumerId, System.currentTimeMillis());

        rebalance(group, topic.getPartitionCount());

        return group;
    }

    public synchronized void leave(String groupName, String consumerId){
        ConsumerGroup group = groups.get(groupName);
        group.getConsumers().remove(consumerId);
        group.getLastHeartbeats().remove(consumerId);
        Topic topic = topicService.getTopic(group.getTopic());
        rebalance(group, topic.getPartitionCount());
    }

    public void heartbeat(String groupName, String consumerId){
        ConsumerGroup group = groups.get(groupName);
        group.getLastHeartbeats().put(consumerId, System.currentTimeMillis());
    }

    private void rebalance(ConsumerGroup group, int partitionCount){
        group.getAssignments().clear();
        List<String> consumers = new ArrayList<>(group.getConsumers());
        if(consumers.isEmpty()) return;
        Collections.sort(consumers);

        for(int partition=0; partition<partitionCount; partition++){
            String consumer = consumers.get(partition% consumers.size());
            group.getAssignments().put(partition, consumer);
        }
    }

    public List<Integer> getAssignedPartitions(String groupName, String consumerId){
        ConsumerGroup group = groups.get(groupName);
        return group.getAssignments().entrySet().stream().filter(entry -> entry.getValue().equals(consumerId))
                .map(Map.Entry::getKey).sorted().toList();
    }

    public Map<Integer, List<Message>> consume(String groupName, String consumerId, int limit){
        ConsumerGroup group = groups.get(groupName);
        List<Integer> partitions = getAssignedPartitions(groupName, consumerId);

        Map<Integer, List<Message>> result = new LinkedHashMap<>();

        for(Integer partition: partitions){
            long commitedOffset = getCommitedOffset(groupName, partition);
            long nextOffset = commitedOffset + 1;
            List<Message> messages = topicService.consume(
                    group.getTopic(),
                    partition,
                    nextOffset,
                    limit
            );

            result.put(partition, messages);
        }

        return result;
    }

    public Map<Integer, List<Message>> longPoll(String groupName, String consumerId, int limit, long timeoutMs){
        long deadline = System.currentTimeMillis() + timeoutMs;
        while(System.currentTimeMillis() < deadline){
            Map<Integer, List<Message>> messages =
                    consume(groupName, consumerId, limit);

            boolean hasMessages = messages.values().stream().anyMatch(list -> !list.isEmpty());

            if(!hasMessages){
                return messages;
            }

            try{
                Thread.sleep(200);
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();
                break;
            }
        }

        return consume(groupName, consumerId, limit);
    }

    public void commit(String groupName, int partition, long offset){
        ConsumerGroup group = groups.get(groupName);
        group.getCommitedOffsets().put(partition, offset);
    }

    public long getCommitedOffset(String groupName, int partition){
        ConsumerGroup group = groups.get(groupName);
        return group.getCommitedOffsets().getOrDefault(partition, -1L);
    }

    public ConsumerGroup getGroup(String groupName){
        return groups.get(groupName);
    }

    @Scheduled(fixedRate = 5000)
    public void removeDeadConsumers(){
        long now = System.currentTimeMillis();
        for(ConsumerGroup group: groups.values()){
            List<String> deadConsumers = group.getLastHeartbeats()
                    .entrySet()
                    .stream()
                    .filter(entry -> now - entry.getValue() > HEARTBEAT_TIMEOUT_MS)
                    .map(Map.Entry::getKey)
                    .toList();

            for(String consumerId: deadConsumers){
                System.out.println("Consumer timed out: " + consumerId + " Group= "+ group.getName());

                group.getConsumers().remove(consumerId);
                group.getLastHeartbeats().remove(consumerId);
                Topic topic = topicService.getTopic(group.getTopic());
                rebalance(group, topic.getPartitionCount());
            }
        }
    }
}
