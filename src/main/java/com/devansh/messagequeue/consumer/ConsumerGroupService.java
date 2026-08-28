package com.devansh.messagequeue.consumer;

import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerGroupService {
    private final ConcurrentHashMap<String, Long> offsets = new ConcurrentHashMap<>();
    public void commit(String group, String topic, int partition, long offset){
        String key = group+":"+topic+":"+partition;
        offsets.put(key, offset);
    }

    public long getCommitedOffset(String group, String topic, int partition){
        String key = group+":"+topic+":"+partition;
        return offsets.getOrDefault(key, -1L);
    }
}
