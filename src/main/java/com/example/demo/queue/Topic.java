package com.example.demo.queue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Topic<T> {

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

    public Subscription subscribe(String group) {
        SubscriptionGroup sg = subscriptionGroups.computeIfAbsent(group, (k) -> new SubscriptionGroup(this));
        return  sg.createSubscription();;
    }
}
