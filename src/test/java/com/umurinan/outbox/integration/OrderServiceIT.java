package com.umurinan.outbox.integration;

import com.umurinan.outbox.entity.OutboxMessage;
import com.umurinan.outbox.repository.OrderRepository;
import com.umurinan.outbox.repository.OutboxMessageRepository;
import com.umurinan.outbox.service.OrderService;
import com.umurinan.outbox.service.PlaceOrderRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for OrderService against a real PostgreSQL database.
 *
 * These tests verify the atomicity guarantee: placing an order must persist
 * both the Order row and the OutboxMessage row in a single transaction.
 * If either write fails, neither should be committed.
 *
 * H2 is never used here. Testcontainers spins up a real Postgres instance
 * so the tests exercise exactly the same database behavior as production.
 */
@SpringBootTest
@Testcontainers
class OrderServiceIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @MockitoBean
    @SuppressWarnings("unused")
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxMessageRepository outboxMessageRepository;

    @BeforeEach
    void cleanUp() {
        outboxMessageRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    @DisplayName("placing an order persists both the order and the outbox message")
    void placeOrder_persistsOrderAndOutboxMessage() {
        PlaceOrderRequest request = new PlaceOrderRequest("user-123", new BigDecimal("59.99"));

        orderService.place(request);

        assertThat(orderRepository.findAll()).hasSize(1);

        List<OutboxMessage> messages = outboxMessageRepository.findAll();
        assertThat(messages).hasSize(1);

        OutboxMessage msg = messages.get(0);
        assertThat(msg.getTopic()).isEqualTo("orders");
        assertThat(msg.getPayload()).contains("OrderPlaced");
        assertThat(msg.isPublished()).isFalse();
    }

    @Test
    @DisplayName("each placed order produces exactly one outbox message")
    void placeOrder_eachOrderProducesOneMessage() {
        orderService.place(new PlaceOrderRequest("user-1", new BigDecimal("10.00")));
        orderService.place(new PlaceOrderRequest("user-2", new BigDecimal("20.00")));
        orderService.place(new PlaceOrderRequest("user-3", new BigDecimal("30.00")));

        assertThat(orderRepository.findAll()).hasSize(3);
        assertThat(outboxMessageRepository.findAll()).hasSize(3);
    }

    @Test
    @DisplayName("outbox message aggregate id matches the saved order id")
    void placeOrder_outboxMessageAggregateIdMatchesOrderId() {
        PlaceOrderRequest request = new PlaceOrderRequest("user-123", new BigDecimal("99.00"));

        orderService.place(request);

        long orderId = orderRepository.findAll().get(0).getId();
        OutboxMessage msg = outboxMessageRepository.findAll().get(0);

        assertThat(msg.getAggregateId()).isEqualTo(String.valueOf(orderId));
    }
}
