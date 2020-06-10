package com.example.service;

import com.example.domain.LibraryEvent;
import com.example.jpa.LibraryEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;

@Slf4j
@Service
@AllArgsConstructor
public class LibraryEventsService {

    private ObjectMapper mapper;
    private LibraryEventRepository repository;
    private KafkaTemplate<Integer, String> kafkaTemplate;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = mapper.readValue(consumerRecord.value(), LibraryEvent.class);

        if (libraryEvent.getLibraryEventId() == 111 ){
            throw new RecoverableDataAccessException("temporary network issue");
        }

        switch (libraryEvent.getLibraryEventType()){
            case NEW:
                repository.save(libraryEvent);
                log.info("library event saved");
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                log.info("library event updated");
                break;
            default: log.error("Event type is not correct");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId()==null){
            throw new IllegalArgumentException("Library Event Id is missing");
        }

        Optional<LibraryEvent> libraryEventOptional = repository.findById(libraryEvent.getLibraryEventId());
        if(!libraryEventOptional.isPresent()){
            throw new IllegalArgumentException("Not a valid library Event");
        }
        log.info("Validation is successful for the library Event : {} ", libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);
    }

    public void handleRecovery(ConsumerRecord<Integer, String> record){

        Integer key = record.key();
        String msg = record.value();

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, msg);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.info("message send with error for key {} with calue {} for partition {}",key, msg, throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                log.info("message send successfully for key {} with calue {} for partition {}",key, msg, result.getRecordMetadata().partition());
            }
        });

    }
}
