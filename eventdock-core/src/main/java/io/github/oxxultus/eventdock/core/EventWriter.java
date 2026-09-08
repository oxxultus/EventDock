package io.github.oxxultus.eventdock.core;

@FunctionalInterface
public interface EventWriter {
  void write(EventEnvelope<?> event);
}
