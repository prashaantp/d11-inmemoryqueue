package com.example.demo;

import com.example.demo.queue.InMemoryQueue;
import com.example.demo.queue.QueueMessage;
import com.example.demo.queue.Subscription;

public class Example1 {

    public static void main(String[] args) {
        InMemoryQueue<String> queue = new InMemoryQueue<>(1);
        queue.addTopic("topic1", 1);
        queue.addTopic("topic2", 1);

        queue.addMessage("topic1", "ex1");
        queue.addMessage("topic1", "ex2");
        queue.addMessage("topic2", "ex3");
        queue.addMessage("topic2", "ex4");

        // first consumer
        Subscription<String> subscription1 = queue.subscribe("topic1", "group1");
        for (QueueMessage<String> m : subscription1.poll()) {
            System.out.println("Consumer1:group1 - " + m);
        }

    }
}
