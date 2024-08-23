package com.example.demo.queue;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class TopicPartition<T> {

    private final ConcurrentLinkedQueue<QueueMessage<T>> messageQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong count = new AtomicLong(0);
    private final int partitionKey;

    public TopicPartition(int partitionKey) {
        this.partitionKey = partitionKey;
    }

    public void addMessage(T message) {
        QueueMessage<T> m = new QueueMessage<>(
                count.incrementAndGet(),
                message,
                System.currentTimeMillis(),
                partitionKey
        );

        messageQueue.add(m);
    }

    public Optional<QueueMessage<T>> poll(int offset) {
        return messageQueue.stream().filter(m -> m.id() > offset).findFirst();
    }
}
