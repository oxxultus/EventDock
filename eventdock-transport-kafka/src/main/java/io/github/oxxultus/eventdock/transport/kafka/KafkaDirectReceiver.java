package io.github.oxxultus.eventdock.transport.kafka;

import io.github.oxxultus.eventdock.core.DirectEventProcessor;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class KafkaDirectReceiver {
  private final String consumerId;
  private final KafkaEventRecordMapper mapper;
  private final DirectEventProcessor processor;

  public KafkaDirectReceiver(
      String consumerId, KafkaEventRecordMapper mapper, DirectEventProcessor processor) {
    if (consumerId == null || consumerId.isBlank()) {
      throw new IllegalArgumentException("consumerId must not be blank");
    }
    this.consumerId = consumerId;
    this.mapper = Objects.requireNonNull(mapper);
    this.processor = Objects.requireNonNull(processor);
  }

  public void receive(ConsumerRecord<String, byte[]> record) {
    processor.process(consumerId, mapper.fromConsumerRecord(record));
  }
}
