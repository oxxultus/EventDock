package io.github.oxxultus.eventdock.transport.kafka;

import io.github.oxxultus.eventdock.core.SerializedEvent;

@FunctionalInterface
public interface KafkaTopicResolver {
  String resolve(SerializedEvent event);

  static KafkaTopicResolver eventType() {
    return SerializedEvent::type;
  }
}
