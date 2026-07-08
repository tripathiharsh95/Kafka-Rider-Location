package com.demo.consumer;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Configuration
public class KafkaConsumer {
//    @KafkaListener(topics = "my-topic", groupId = "my-new-group")
//    public void listen1(String message){
//        System.out.println("Recieved Message 1: " + message);
//    }
//
//    @KafkaListener(topics = "my-topic", groupId = "my-new-group")
//    public void listen2(String message){
//        System.out.println("Recieved Message 2: " + message);
//    }
//
//    @KafkaListener(topics = "my-topic-new", groupId = "my-new-group-rider")
//    public void listenRiderLocation(RiderLocation location){
//        System.out.println("Recieved Location: " + location.getRiderId() + " : " + location.getLatitude() + " : " + location.getLongitude());
//    }
}
