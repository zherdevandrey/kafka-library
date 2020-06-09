package com.unit;

import com.example.kafkalibrary.producer.KafkaLibraryProducerApplication;
import com.example.kafkalibrary.producer.controller.LibraryEventController;
import com.example.kafkalibrary.producer.domain.Book;
import com.example.kafkalibrary.producer.domain.LibraryEvent;
import com.example.kafkalibrary.producer.producer.LibraryEventProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@WebMvcTest(LibraryEventController.class)
@ContextConfiguration(classes = KafkaLibraryProducerApplication.class)
class LibraryEventControllerTest {

    @Autowired
    MockMvc mockMvc;

    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @MockBean
    LibraryEventProducer producer;

    @Test
    void postLibraryEvent() throws Exception {
        Book book = Book.builder()
                .bookId(123)
                .bookName("test book")
                .bookAuthor("test author")
                .build();
        LibraryEvent libraryEventTest = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book)
                .build();
        String json = objectMapper.writeValueAsString(libraryEventTest);
        doNothing().when(producer).sendLibraryEventAsynchronously(libraryEventTest);
        mockMvc.perform(post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON)
        ).andExpect(status().isCreated());
    }

    @Test
    void postLibraryEvent4xx() throws Exception {
        Book book = Book.builder()
                .bookId(null)
                .bookName("")
                .bookAuthor("author")
                .build();

        LibraryEvent libraryEventTest = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEventTest);
        doNothing().when(producer).sendLibraryEventAsynchronously(libraryEventTest);

        String expected = "book.bookId - must not be null, book.bookName - must not be blank";

        mockMvc.perform(post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON)
        ).andExpect(status().is4xxClientError())
                .andExpect(content().string(expected));
    }
}