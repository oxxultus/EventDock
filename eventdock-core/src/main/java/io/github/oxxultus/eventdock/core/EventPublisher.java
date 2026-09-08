package io.github.oxxultus.eventdock.core;

@FunctionalInterface
public interface EventPublisher {
  void publish(SerializedEvent event);
}
