package com.example.config;

import com.example.service.LibraryEventsService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
@AllArgsConstructor
public class KafkaLibraryConsumerConfig {

    LibraryEventsService service;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setRetryTemplate(retryTemplate());
        factory.setRecoveryCallback(context -> {
            if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException){
                log.error("recoverable exception");
//                Arrays.asList(context.attributeNames())
//                        .forEach(att -> {
//                            log.info("attribute name: {}" + att);
//                            log.info("attribute name: {}" + context.getAttribute(att));
//                        });

                ConsumerRecord consumerRecord = (ConsumerRecord)context.getAttribute("record");
                service.handleRecovery(consumerRecord);


                return null;
            }else {
                log.error("unrecoverable exception");
                throw new RuntimeException(context.getLastThrowable());
            }
        });
        return factory;
    }
//
//    @Bean
//    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
//            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
//            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
//        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        configurer.configure(factory, kafkaConsumerFactory);
//        factory.setConcurrency(3);
//        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
//        factory.setErrorHandler(((thrownException, data) -> {
//            log.info("Exception in consumerConfig is {} and the record is {}", thrownException.getMessage(), data);
//            //persist
//        }));
//        factory.setRetryTemplate(retryTemplate());
//
//        return factory;
//    }

    private RetryTemplate retryTemplate() {
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return  retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
        /*SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(3);*/
        Map<Class<? extends Throwable>, Boolean> exceptionsMap = new HashMap<>();
        exceptionsMap.put(IllegalArgumentException.class, false);
        exceptionsMap.put(RecoverableDataAccessException.class, true);
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3,exceptionsMap,true);
        return simpleRetryPolicy;
    }
}
