package com.devansh.messagequeue.topic;

import com.devansh.messagequeue.message.Message;
import com.devansh.messagequeue.message.ProduceMessageResponse;
import com.devansh.messagequeue.partition.Partition;
import com.devansh.messagequeue.partition.Partitioner;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TopicService {
    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final Partitioner partitioner;
    public TopicService(Partitioner partitioner) {
        this.partitioner = partitioner;
    }
    public void createTopic(String name, int partitionCount){
        if(partitionCount <= 0){
            throw new IllegalArgumentException("Partition count must be greater than 0");
        }
        Topic topic = new Topic(name, partitionCount);
        Topic existing = topics.putIfAbsent(name, topic);
        if(existing != null){
            throw new IllegalArgumentException("Topic already exists");
        }
    }

    public ProduceMessageResponse produce(String topicName, String key, String value){
        Topic topic = topics.get(topicName);
        int partitionId = partitioner.select(key, topic.getPartitionCount());
        Partition partition = topic.getPartition(partitionId);
        Message message = partition.append(key, value, topicName);

        return new ProduceMessageResponse(topicName, partitionId, message.offset());
    }

    public List<Message> consume(String topicName, int partitionId, long offset, int limit){
        Topic topic = topics.get(topicName);

        return topic.getPartition(partitionId).read(offset, limit);
    }

    public Topic getTopic(String topicName){
        return topics.get(topicName);
    }

    public List<TopicInfo> getTopics(){
        return topics.values()
                .stream()
                .map(this::toTopicInfo)
                .sorted(
                        java.util.Comparator.comparing(
                                TopicInfo::name
                        )
                )
                .toList();
    }

    public TopicInfo getTopicInfo(String topicName){
        Topic topic = topics.get(topicName);

        if(topic == null){
            throw new IllegalArgumentException(
                    "Topic not found: " + topicName
            );
        }

        return toTopicInfo(topic);
    }

    private TopicInfo toTopicInfo(Topic topic){

        List<PartitionInfo> partitions =
                topic.getPartitions()
                        .stream()
                        .map(partition ->
                                new PartitionInfo(
                                        partition.getId(),
                                        partition.getNextOffset()
                                )
                        )
                        .toList();

        return new TopicInfo(
                topic.getTopicName(),
                topic.getPartitionCount(),
                partitions
        );
    }

    public int getTopicCount(){
        return topics.size();
    }

    public int getTotalPartitionCount(){
        return topics.values()
                .stream()
                .mapToInt(Topic::getPartitionCount)
                .sum();
    }

    public long getTotalMessageCount(){
        return topics.values()
                .stream()
                .flatMap(topic ->
                        topic.getPartitions().stream()
                )
                .mapToLong(partition ->
                        partition.getNextOffset()
                )
                .sum();
    }

    public int selectPartition(String topicName, String key){
        Topic topic = getTopic(topicName);
        return partitioner.select(key, topic.getPartitionCount());
    }

    public Message appendToPartition(
            String topicName,
            int partitionId,
            String key,
            String value
    ){
        Topic topic = getTopic(topicName);
        if(topic == null){
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        Partition partition = topic.getPartition(partitionId);
        return partition.append(key, value, topicName);
    }

    public ProduceMessageResponse produceToPartition(
            String topicName,
            int partitionId,
            String key,
            String value
    ){
        Message message = appendToPartition(topicName, partitionId, key, value);

        return new ProduceMessageResponse(topicName, partitionId, message.offset());
    }

    public void appendReplica(String topicName, int partitionId, Message message){
        Topic topic = getTopic(topicName);
        if(topic == null){
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        Partition partition = topic.getPartition(partitionId);
        partition.appendReplica(message);
    }
}
