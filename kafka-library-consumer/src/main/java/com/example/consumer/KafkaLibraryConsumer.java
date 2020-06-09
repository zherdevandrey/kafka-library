package com.example.consumer;

import com.example.service.LibraryEventsService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@AllArgsConstructor
public class KafkaLibraryConsumer {

    private LibraryEventsService service;

    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord consumerRecord){
        service.processLibraryEvent(consumerRecord);
    }

}
