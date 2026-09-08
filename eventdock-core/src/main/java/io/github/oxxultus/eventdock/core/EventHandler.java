package io.github.oxxultus.eventdock.core;

@FunctionalInterface
public interface EventHandler {
  void handle(SerializedEvent event);
}
