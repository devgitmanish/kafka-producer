package com.kafka.service;

import com.kafka.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void sendMessageToTopic(String message) {
        /*
        CompletableFuture<SendResult<String, Object>> future
                = kafkaTemplate.send("devin-topic", message);
        */
        /* Topic created by java file using NewTopic class*/
        CompletableFuture<SendResult<String, Object>> future
                = kafkaTemplate.send("devin-topic-2", message);


        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent Message =[" + message + "] " +
                        "with offset=[" + result.getRecordMetadata().offset() + " ]");
            } else {
                System.out.println("Unablt to Send Message =[" + message + "] " +
                        "due to " + ex.getMessage());
            }
        });
    }

    public void sendEmployeeObjectToTopic(Customer customer) {

        System.out.println("Message sending from producer....");

        try {
            CompletableFuture<SendResult<String, Object>> future
                    = kafkaTemplate.send("devin-topic-obj-cust", customer);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent Message =[" + customer.toString() + "] " +
                            "with offset=[" + result.getRecordMetadata().offset() + " ]");
                } else {
                    System.out.println("Unable to Send Message =[" + customer.toString() + "] " +
                            "due to " + ex.getMessage());
                }
            });
        } catch (Exception ex) {
            System.out.println("Error " + ex.getMessage());
        }
    }

    public void sendEmployeeObjectToPirticularPartation(Customer customer) {

        System.out.println("Message sending from producer....");

        try {
            CompletableFuture<SendResult<String, Object>> future
                    = kafkaTemplate.send("devin-topic-obj-cust", 2, null, customer);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent Message =[" + customer.toString() + "] " +
                            "with offset=[" + result.getRecordMetadata().offset() + " ]");
                } else {
                    System.out.println("Unable to Send Message =[" + customer.toString() + "] " +
                            "due to " + ex.getMessage());
                }
            });
        } catch (Exception ex) {
            System.out.println("Error " + ex.getMessage());
        }
    }
}
