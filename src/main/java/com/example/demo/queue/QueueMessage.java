package com.example.demo.queue;

public record QueueMessage<T>(
        long id,
        T message,
        long ttl,
        int partitionKey
) {
}
