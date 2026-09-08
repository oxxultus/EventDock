package io.github.oxxultus.eventdock.inbox;

import io.github.oxxultus.eventdock.core.EventHandlerRegistry;

@FunctionalInterface
public interface InboxHandlerRegistry extends EventHandlerRegistry {
  @Override
  InboxHandler get(String consumerId, String eventType);
}
