package com.devansh.messagequeue.message;

public record ProduceMessageRequest (
    String key,
    String value,
    String topic
){}
