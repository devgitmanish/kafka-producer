package com.kafka.controller;

import com.kafka.dto.Customer;
import com.kafka.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka-producer")
public class EventKafkaController {

    @Autowired
    private KafkaMessagePublisher kafkaMessagePublisher;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try {
            kafkaMessagePublisher.sendMessageToTopic(message);
            return ResponseEntity.ok("message published successfully....");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @PostMapping("/publish/event")
    public ResponseEntity<?> sendEventAsObject(@RequestBody Customer customer){
        try{
            kafkaMessagePublisher.sendEmployeeObjectToTopic(customer);
            return ResponseEntity.ok("message event published successfully...");
        }catch (Exception ex){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
