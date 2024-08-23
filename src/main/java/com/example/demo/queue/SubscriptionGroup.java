package com.example.demo.queue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionGroup<T> {
    private final Topic<T> topic;

    private final Map<Integer, Subscription> subscriptions = new ConcurrentHashMap<>();

    public SubscriptionGroup(Topic<T> topic) {
        this.topic = topic;
    }

    public Subscription<T> createSubscription() {
        return new Subscription<T>(this);
    }
}
