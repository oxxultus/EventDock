package io.github.oxxultus.eventdock.inbox;

@FunctionalInterface
public interface InboxHandlerRegistry {
  InboxHandler get(String consumerId, String eventType);
}
