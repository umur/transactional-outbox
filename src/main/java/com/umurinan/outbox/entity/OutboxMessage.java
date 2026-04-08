package com.umurinan.outbox.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(name = "outbox")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Kafka topic to publish to.
     */
    @Column(nullable = false)
    private String topic;

    /**
     * Kafka message key - used for partition routing.
     * Typically the aggregate id so all events for the same aggregate
     * land on the same partition and stay ordered.
     */
    @Column(nullable = false)
    private String aggregateId;

    /**
     * JSON payload to publish as the Kafka message value.
     */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    /**
     * False until the relay successfully publishes to Kafka.
     */
    @Column(nullable = false)
    @Builder.Default
    private boolean published = false;

    @Column(nullable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();
}
