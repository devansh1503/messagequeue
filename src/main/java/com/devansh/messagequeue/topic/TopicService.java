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
}
