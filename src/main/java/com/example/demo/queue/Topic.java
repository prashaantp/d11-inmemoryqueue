package com.example.demo.queue;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Topic<T> {

    private final int maxConsumeMessages = 5;

    private final String name;

    private final int partitionCount;
    private final TopicPartition<T>[] partitions;

    private final Map<String, SubscriptionGroup<T>> subscriptionGroups = new ConcurrentHashMap<>();

    public Topic(String name, int partitionCount) {
        this.name = name;
        this.partitions = new TopicPartition[partitionCount];
        this.partitionCount = partitionCount;
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new TopicPartition<>(i);
        }
    }

    public String getName() {
        return name;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public TopicPartition<T>[] getPartitions() {
        return partitions;
    }

    public void addMessage(T message) {
        this.partitions[message.hashCode() % partitionCount].addMessage(message);
    }

    public Subscription<T> subscribe(String group) {
        SubscriptionGroup<T> sg = subscriptionGroups.computeIfAbsent(group, (k) -> new SubscriptionGroup<T>(this));
        return sg.createSubscription();
    }

    public List<QueueMessage<T>> poll(Map<Integer, Long> offsets) {
        List<QueueMessage<T>> result = new LinkedList<>();

        for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
            int partitionKey = entry.getKey();
            long offset = entry.getValue();

            result.addAll(partitions[partitionKey].poll(offset, maxConsumeMessages));

            if (result.size() >= maxConsumeMessages) {
                return result;
            }
        }

        return result;
    }
}
