package com.example.demo.queue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SubscriptionGroup<T> {
    private final Map<Integer, Subscription<T>> subscriptions = new ConcurrentHashMap<>();

    private final Topic<T> topic;

    public SubscriptionGroup(Topic<T> topic) {
        this.topic = topic;
    }

    public Topic<T> getTopic() {
        return topic;
    }

    public Map<Integer, Subscription<T>> getSubscriptions() {
        return subscriptions;
    }

    public Subscription<T> createSubscription() {
        Set<Integer> partitionKeys =
                IntStream.range(0, topic.getPartitionCount())
                        .boxed()
                        .collect(Collectors.toSet());

        Subscription<T> sub = new Subscription<>(this, partitionKeys);
        subscriptions.replaceAll((k, v) -> sub);
        return sub;
    }

    public List<QueueMessage<T>> poll(Map<Integer, Long> offsets) {
        return topic.poll(offsets);
    }
}
