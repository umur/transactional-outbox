package com.umurinan.outbox.repository;

import com.umurinan.outbox.entity.OutboxMessage;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {

    List<OutboxMessage> findByPublishedFalseOrderByCreatedAtAsc();

    List<OutboxMessage> findByPublishedFalse();
}
