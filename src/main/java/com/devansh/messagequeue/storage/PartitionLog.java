package com.devansh.messagequeue.storage;

import com.devansh.messagequeue.message.Message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class PartitionLog {
    private final Path path;
    public PartitionLog(String topic, int partitionId){
        try{
            Path directory = Paths.get("data", topic);
            Files.createDirectories(directory);
            this.path = directory.resolve("partition-"+partitionId+".log");

            if(!Files.exists(path)){
                Files.createFile(path);
            }
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    public synchronized void append(Message message){
        String line = buildMessageLine(message);
        try{
            Files.writeString(
                    path,
                    line+System.lineSeparator(),
                    StandardOpenOption.APPEND
            );
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    public List<Message> readFrom(long offset, int limit){
        try{
            List<String> lines = Files.readAllLines(path);
            List<Message> messages = new ArrayList<>();

            for(String line : lines){
                Message message = parse(line);

                if(message.offset() >= offset){
                    messages.add(message);
                }

                if(messages.size() == limit) break;
            }

            return messages;
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    public long getNextOffset(){
        try{
            long count = Files.lines(path).count();
            return count;
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    public String safe(String value){
        return value == null ? "" : value.replace("\n", "\\n");
    }

    public String restore(String value){
        return value.replace("\\n", "\n");
    }

    public Message parse(String line){
        String[]parts =  line.split("\\|");

        return new Message(
                restore(parts[4]),
                restore(parts[5]),
                parts[2],
                Integer.parseInt(parts[3]),
                Long.parseLong(parts[0]),
                Long.parseLong(parts[1]),
                Integer.parseInt(parts[6]),
                Integer.parseInt(parts[7])
        );
    }

    public String buildMessageLine(Message message){
        return (
                message.offset()+"|"+
                        message.timestamp()+"|"+
                        message.topic()+"|"+
                        message.partition()+"|"+
                        safe(message.key())+"|"+
                        safe(message.value())+"|"+
                        message.size()+"|"+
                        message.crc()
        );
    }
}
