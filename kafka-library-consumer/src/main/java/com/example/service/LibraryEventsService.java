package com.example.service;

import com.example.domain.LibraryEvent;
import com.example.jpa.LibraryEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Slf4j
@Service
@AllArgsConstructor
public class LibraryEventsService {

    private ObjectMapper mapper;
    private LibraryEventRepository repository;

    @SneakyThrows
    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord){
        LibraryEvent libraryEvent = mapper.readValue(consumerRecord.value(), LibraryEvent.class);
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
}
