package com.devansh.messagequeue.partition;

import com.devansh.messagequeue.message.Message;
import com.devansh.messagequeue.storage.PartitionLog;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Partition {
    private final int id;
    private final PartitionLog log;
    private final AtomicLong nextOffset;
    public Partition(String topic, int id){
        this.id=id;
        this.log = new PartitionLog(topic, id);
        this.nextOffset = new AtomicLong(log.getNextOffset());
    }

    public Message append(String key, String value, String topic){
        long offset = nextOffset.getAndIncrement();
        Message message = new Message(
                key,
                value,
                topic,
                id,
                offset,
                System.currentTimeMillis(),
                1,
                1
        );

        log.append(message);
        return message;
    }

    public List<Message> read(long offset, int limit){
        return log.readFrom(offset, limit);
    }

    public int getId(){
        return id;
    }

    public long getNextOffset(){
        return nextOffset.get();
    }

    public void appendReplica(Message message){
        log.append(message);
        nextOffset.set(
                Math.max(nextOffset.get(), message.offset()+1)
        );
    }
}
