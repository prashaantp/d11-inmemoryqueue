package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.Time;
import java.util.concurrent.*;


//Problem Statement
//
//Design and implement an in-memory Queue.
//You can create multiple topics and push messages to each topic.
//Messages can be of any form.
//Consumers can subscribe to a topic and start consuming messages.

@SpringBootApplication
public class DemoApplication {



	public static void main(String[] args) {



		SpringApplication.run(DemoApplication.class, args);
	}

}
