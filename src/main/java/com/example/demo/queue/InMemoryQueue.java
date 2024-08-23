package com.example.demo.queue;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryQueue<T> {
    private final ConcurrentHashMap<String, Topic<T>> topics = new ConcurrentHashMap<>();

    private final int defaultPartitions;

    public InMemoryQueue(int defaultPartitions) {
        this.defaultPartitions = defaultPartitions;
    }

    private Topic<T> getTopic(String name, int partitions) {
        return topics.computeIfAbsent(name, (k) -> new Topic<T>(name, partitions));
    }

    public void addTopic(String name, int partitions) {
        getTopic(name, partitions);
    }

    public void addMessage(String topic, T message) {
        Topic<T> topicObj = topics.get(topic);
        if (topicObj != null) {
            topicObj.addMessage(message);
        }
    }

    public Subscription<T> subscribe(String name, String group) {
        Topic<T> topic = getTopic(name, defaultPartitions);
        return topic.subscribe(group);
    }

}
