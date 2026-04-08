package com.umurinan.outbox.service;

import com.umurinan.outbox.entity.Order;
import com.umurinan.outbox.entity.OutboxMessage;
import com.umurinan.outbox.repository.OrderRepository;
import com.umurinan.outbox.repository.OutboxMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for OrderService.
 *
 * These tests verify that placing an order saves both an Order and an OutboxMessage.
 * No Spring context, no database - pure logic verification with mocked repositories.
 */
@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OutboxMessageRepository outboxMessageRepository;

    private OrderService orderService;

    private PlaceOrderRequest request;

    @BeforeEach
    void setUp() {
        orderService = new OrderService(orderRepository, outboxMessageRepository);
        request = new PlaceOrderRequest("user-123", new BigDecimal("59.99"));
    }

    @Test
    @DisplayName("placeOrder saves an Order to the order repository")
    void placeOrder_savesOrder() {
        Order saved = Order.builder()
                .id(1L)
                .userId("user-123")
                .total(new BigDecimal("59.99"))
                .build();
        when(orderRepository.save(any(Order.class))).thenReturn(saved);

        orderService.place(request);

        ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);
        verify(orderRepository).save(captor.capture());
        assertThat(captor.getValue().getUserId()).isEqualTo("user-123");
        assertThat(captor.getValue().getTotal()).isEqualByComparingTo("59.99");
    }

    @Test
    @DisplayName("placeOrder saves an OutboxMessage in the same call")
    void placeOrder_savesOutboxMessage() {
        Order saved = Order.builder()
                .id(1L)
                .userId("user-123")
                .total(new BigDecimal("59.99"))
                .build();
        when(orderRepository.save(any(Order.class))).thenReturn(saved);

        orderService.place(request);

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxMessageRepository).save(captor.capture());
        OutboxMessage msg = captor.getValue();
        assertThat(msg.getTopic()).isEqualTo("orders");
        assertThat(msg.getAggregateId()).isEqualTo("1");
        assertThat(msg.getPayload()).contains("OrderPlaced");
        assertThat(msg.isPublished()).isFalse();
    }

    @Test
    @DisplayName("placeOrder returns a response with the saved order id")
    void placeOrder_returnsOrderId() {
        Order saved = Order.builder()
                .id(42L)
                .userId("user-123")
                .total(new BigDecimal("59.99"))
                .build();
        when(orderRepository.save(any(Order.class))).thenReturn(saved);

        OrderResponse response = orderService.place(request);

        assertThat(response.orderId()).isEqualTo(42L);
        assertThat(response.userId()).isEqualTo("user-123");
    }
}
