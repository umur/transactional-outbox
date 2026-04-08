package com.umurinan.outbox.relay;

import com.umurinan.outbox.entity.OutboxMessage;
import com.umurinan.outbox.repository.OutboxMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Polls the outbox table for unpublished messages and forwards them to Kafka.
 *
 * The relay is the second half of the transactional outbox pattern. The first half
 * (writing the outbox row atomically with the business write) happens in OrderService.
 * This component reads those rows and hands them off to Kafka.
 *
 * Delivery guarantee: at-least-once. If the relay sends the message to Kafka but
 * crashes before marking the row published, it will send the same message again on
 * the next run. Consumers must be idempotent.
 *
 * The @Scheduled method calls process() which can also be called directly in tests.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxRelay {

    private final OutboxMessageRepository outboxMessageRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelayString = "${outbox.relay.interval-ms:500}")
    public void scheduledProcess() {
        process();
    }

    @Transactional
    public void process() {
        List<OutboxMessage> pending = outboxMessageRepository.findByPublishedFalseOrderByCreatedAtAsc();

        if (pending.isEmpty()) {
            return;
        }

        log.debug("Processing {} outbox message(s)", pending.size());

        for (OutboxMessage message : pending) {
            kafkaTemplate.send(message.getTopic(), message.getAggregateId(), message.getPayload())
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish outbox message id={}", message.getId(), ex);
                        }
                    });

            message.setPublished(true);
            outboxMessageRepository.save(message);
        }
    }
}
