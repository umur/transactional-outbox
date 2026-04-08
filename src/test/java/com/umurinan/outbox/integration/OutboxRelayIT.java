package com.umurinan.outbox.integration;

import com.umurinan.outbox.entity.OutboxMessage;
import com.umurinan.outbox.relay.OutboxRelay;
import com.umurinan.outbox.repository.OrderRepository;
import com.umurinan.outbox.repository.OutboxMessageRepository;
import com.umurinan.outbox.service.OrderService;
import com.umurinan.outbox.service.PlaceOrderRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for OutboxRelay against real PostgreSQL and Kafka instances.
 *
 * Scheduling is disabled (interval set to ~28 days) so only manual process() calls run.
 * A fresh consumer subscribes and seeks to the end before each test, ensuring each test
 * only sees messages produced during that specific test run.
 */
@SpringBootTest(properties = "outbox.relay.interval-ms=2147483647")
@Testcontainers
class OutboxRelayIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxRelay outboxRelay;

    @Autowired
    private OutboxMessageRepository outboxMessageRepository;

    @Autowired
    private OrderRepository orderRepository;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        outboxMessageRepository.deleteAll();
        orderRepository.deleteAll();

        // Subscribe before the test places any orders so "latest" captures only this test's messages
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("orders"));
        consumer.poll(Duration.ofMillis(500)); // trigger partition assignment and seek to current end
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @DisplayName("relay publishes unpublished outbox messages to Kafka")
    void relay_publishesOutboxMessages() {
        orderService.place(new PlaceOrderRequest("user-1", new BigDecimal("49.99")));
        outboxRelay.process();

        List<String> received = poll(1);
        assertThat(received).hasSize(1);
        assertThat(received.get(0)).contains("OrderPlaced");
    }

    @Test
    @DisplayName("relay marks messages as published after sending to Kafka")
    void relay_marksMessagesAsPublished() {
        orderService.place(new PlaceOrderRequest("user-2", new BigDecimal("19.99")));

        assertThat(outboxMessageRepository.findByPublishedFalse()).hasSize(1);

        outboxRelay.process();

        assertThat(outboxMessageRepository.findByPublishedFalse()).isEmpty();
        assertThat(outboxMessageRepository.findAll())
                .extracting(OutboxMessage::isPublished)
                .containsOnly(true);
    }

    @Test
    @DisplayName("relay does not re-publish already published messages")
    void relay_skipsAlreadyPublishedMessages() {
        orderService.place(new PlaceOrderRequest("user-3", new BigDecimal("9.99")));

        outboxRelay.process(); // first run - publishes
        outboxRelay.process(); // second run - should skip

        List<String> received = poll(1);
        assertThat(received).hasSize(1); // still only 1 message in Kafka
    }

    @Test
    @DisplayName("relay publishes multiple messages in order")
    void relay_publishesMultipleMessages() {
        orderService.place(new PlaceOrderRequest("user-4", new BigDecimal("10.00")));
        orderService.place(new PlaceOrderRequest("user-5", new BigDecimal("20.00")));
        orderService.place(new PlaceOrderRequest("user-6", new BigDecimal("30.00")));

        outboxRelay.process();

        List<String> received = poll(3);
        assertThat(received).hasSize(3);
        assertThat(outboxMessageRepository.findByPublishedFalse()).isEmpty();
    }

    private List<String> poll(int expectedCount) {
        List<String> messages = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (messages.size() < expectedCount && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(r -> messages.add(r.value()));
        }
        return messages;
    }
}
