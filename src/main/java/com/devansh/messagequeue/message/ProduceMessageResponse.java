package com.devansh.messagequeue.message;

import lombok.AllArgsConstructor;

public record ProduceMessageResponse (
    String topic,
    int partition,
    long offset
){}
