package io.github.oxxultus.eventdock.core;

@FunctionalInterface
public interface EventHandlerRegistry {
  EventHandler get(String consumerId, String eventType);
}
