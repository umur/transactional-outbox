package com.umurinan.outbox.repository;

import com.umurinan.outbox.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
}
