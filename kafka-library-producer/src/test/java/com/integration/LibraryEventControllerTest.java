package com.integration;

import com.example.kafkalibrary.producer.KafkaLibraryProducerApplication;
import com.example.kafkalibrary.producer.domain.Book;
import com.example.kafkalibrary.producer.domain.LibraryEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = "library-events", partitions = 3)
@TestPropertySource(properties = "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}, " +
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}")
@ContextConfiguration(classes = KafkaLibraryProducerApplication.class)
@RequiredArgsConstructor
class LibraryEventControllerTest {

    private final TestRestTemplate testRestTemplate;
    private final EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("", "", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void testDown(){
        consumer.close();
    }

    @Test
    void postLibraryEventAsynchronously() {
        Book book = Book.builder()
                .bookId(123)
                .bookName("test book")
                .bookAuthor("test author")
                .build();

        LibraryEvent libraryEventTest = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book)
                .build();
        HttpEntity<LibraryEvent> entity = new HttpEntity<>(libraryEventTest);

        ResponseEntity<LibraryEvent> result = testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST, entity, LibraryEvent.class);

        HttpStatus statusCode = result.getStatusCode();
        assertEquals(statusCode, HttpStatus.CREATED);

        ConsumerRecord<Integer, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");

        Integer key = singleRecord.key();
        String expected = "{\"libraryEventId\": 778,\"book\": {\"bookId\": 777,\"bookName\": \"some book\",\"bookAuthor\": \"book author\"}}";
        String actual = singleRecord.value();
        assertEquals(expected, actual);
    }
}