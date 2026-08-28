package com.devansh.messagequeue.partition;

import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
public class Partitioner {
    public int select(String key, int partitionCount){
        if(key == null || key.isBlank()){
            return ThreadLocalRandom.current().nextInt(partitionCount);
        }
        return Math.floorMod(key.hashCode(), partitionCount);
    }
}
