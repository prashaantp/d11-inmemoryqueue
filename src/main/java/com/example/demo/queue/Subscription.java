package com.example.demo.queue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Subscription<T> {

    private final Map<Integer, Long> offsets = new ConcurrentHashMap<>();

    private final SubscriptionGroup<T> group;

    public Subscription(SubscriptionGroup<T> group, Set<Integer> partitionKeys) {
        this.group = group;
        partitionKeys.forEach(key -> offsets.put(key, 0L));
    }

    public Map<Integer, Long> getOffsets() {
        return offsets;
    }

    public List<QueueMessage<T>> poll() {
        List<QueueMessage<T>> messages = group.poll(offsets);
        messages.forEach(m -> offsets.compute(m.partitionKey(), (k, v) -> Math.max(m.id(), v == null ? 0 : v)));
        return messages;
    }
}
