package io.github.oxxultus.eventdock.transport.kafka;

import io.github.oxxultus.eventdock.inbox.InboxRepository;
import java.time.Clock;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class KafkaInboxReceiver {
  private final String consumerId;
  private final KafkaEventRecordMapper mapper;
  private final InboxRepository repository;
  private final Clock clock;

  public KafkaInboxReceiver(
      String consumerId,
      KafkaEventRecordMapper mapper,
      InboxRepository repository,
      Clock clock) {
    if (consumerId == null || consumerId.isBlank()) {
      throw new IllegalArgumentException("consumerId must not be blank");
    }
    this.consumerId = consumerId;
    this.mapper = Objects.requireNonNull(mapper);
    this.repository = Objects.requireNonNull(repository);
    this.clock = Objects.requireNonNull(clock);
  }

  public boolean receive(ConsumerRecord<String, byte[]> record) {
    return repository.receive(consumerId, mapper.fromConsumerRecord(record), clock.instant());
  }
}
