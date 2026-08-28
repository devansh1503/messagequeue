package com.devansh.messagequeue.topic;

import com.devansh.messagequeue.partition.Partition;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@Getter
public class Topic {
    private String topicName;
    private final List<Partition> partitions;
    public Topic(String topicName, int partitionCount){
        this.topicName = topicName;
        this.partitions = IntStream.range(0, partitionCount)
                .mapToObj(id -> new Partition(topicName, id))
                .toList();
    }
    public Partition getPartition(int id){
        if(id < 0 || id >= partitions.size()){
            throw new IllegalArgumentException("Invalid partition id " + id);
        }
        return partitions.get(id);
    }

    public int getPartitionCount(){
        return partitions.size();
    }
}
