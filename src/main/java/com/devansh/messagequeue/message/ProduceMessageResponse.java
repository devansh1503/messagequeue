package com.devansh.messagequeue.message;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ProduceMessageResponse {
    String topic;
    int partition;
    long offset;
}
