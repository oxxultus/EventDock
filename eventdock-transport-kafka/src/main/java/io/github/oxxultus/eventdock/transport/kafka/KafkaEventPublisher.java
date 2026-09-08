package io.github.oxxultus.eventdock.transport.kafka;

import io.github.oxxultus.eventdock.core.EventPublisher;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.util.Objects;

public final class KafkaEventPublisher implements EventPublisher {
  private final KafkaEventRecordMapper mapper;
  private final KafkaSender sender;

  public KafkaEventPublisher(KafkaEventRecordMapper mapper, KafkaSender sender) {
    this.mapper = Objects.requireNonNull(mapper);
    this.sender = Objects.requireNonNull(sender);
  }

  @Override
  public void publish(SerializedEvent event) {
    sender.send(mapper.toProducerRecord(event));
  }
}
