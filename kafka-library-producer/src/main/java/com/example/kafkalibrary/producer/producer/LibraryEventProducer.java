package com.example.kafkalibrary.producer.producer;

import com.example.kafkalibrary.producer.domain.LibraryEvent;
import com.example.kafkalibrary.producer.domain.Book;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Arrays;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class LibraryEventProducer {

    public final KafkaTemplate<Integer, String> kafkaTemplate;
    public final ObjectMapper objectMapper;

    private String topic = "library-events";

    public void sendLibraryEventAsynchronously(LibraryEvent libraryEvent) throws JsonProcessingException {
        int key = libraryEvent.getLibraryEventId();
        Book book = libraryEvent.getBook();

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, objectMapper.writeValueAsString(book));
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.info("message send with error for key {} with calue {} for partition {}",key, book, throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                log.info("message send successfully for key {} with calue {} for partition {}",key, book, result.getRecordMetadata().partition());
            }
        });
    }

    @SneakyThrows
    public void sendLibraryEventOption2(LibraryEvent libraryEvent) {
        Book book = libraryEvent.getBook();
        kafkaTemplate.send(topic, objectMapper.writeValueAsString(libraryEvent));
    }

    @SneakyThrows
    public void sendLibraryEventOption3(LibraryEvent libraryEvent) {
        int key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
        kafkaTemplate.send(producerRecord);
    }

    private ProducerRecord<Integer, String> buildProducerRecord(int key, String value, String topic) {
        List<Header> recordHeaders = Arrays.asList(new RecordHeader("event-source", "scanner".getBytes()),
                                                   new RecordHeader("event-target", "target".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    @SneakyThrows
    public SendResult<Integer, String> sendLibraryEventSynchronously(LibraryEvent libraryEvent) {
        int key = libraryEvent.getLibraryEventId();
        Book book = libraryEvent.getBook();
        log.info("before sending");
        SendResult<Integer, String> integerStringSendResult = kafkaTemplate.sendDefault(key, objectMapper.writeValueAsString(book)).get();
        log.info("after sending");
        return integerStringSendResult;
    }
}
