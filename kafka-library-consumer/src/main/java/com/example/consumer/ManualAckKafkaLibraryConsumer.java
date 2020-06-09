package com.example.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

//@Component
//@Slf4j
public class ManualAckKafkaLibraryConsumer
       // implements AcknowledgingMessageListener<Integer, String>
        {

//
//    @Override
//    //@KafkaListener(topics = "library-events")
//    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
//        log.info("topic " + consumerRecord.topic());
//        log.info("value " + consumerRecord.value());
//        log.info("partition " + consumerRecord.partition());
//        log.info("key " + consumerRecord.key());
//        log.info("acknowledged");
//        acknowledgment.acknowledge();
//    }
}
