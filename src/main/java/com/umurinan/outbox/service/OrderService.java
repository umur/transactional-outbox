package com.umurinan.outbox.service;

import com.umurinan.outbox.entity.Order;
import com.umurinan.outbox.entity.OutboxMessage;
import com.umurinan.outbox.repository.OrderRepository;
import com.umurinan.outbox.repository.OutboxMessageRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxMessageRepository outboxMessageRepository;

    public OrderResponse place(PlaceOrderRequest request) {
        Order order = orderRepository.save(
                Order.builder()
                        .userId(request.userId())
                        .total(request.total())
                        .build()
        );

        outboxMessageRepository.save(
                OutboxMessage.builder()
                        .topic("orders")
                        .aggregateId(String.valueOf(order.getId()))
                        .payload(buildPayload(order))
                        .build()
        );

        return new OrderResponse(order.getId(), order.getUserId(), order.getTotal(), order.getStatus());
    }

    private String buildPayload(Order order) {
        return """
                {"event":"OrderPlaced","orderId":%d,"userId":"%s","total":%s,"status":"%s"}
                """.formatted(order.getId(), order.getUserId(), order.getTotal(), order.getStatus()).strip();
    }
}
