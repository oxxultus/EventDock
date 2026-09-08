package io.github.oxxultus.eventdock.inbox;

import io.github.oxxultus.eventdock.core.SerializedEvent;

@FunctionalInterface
public interface InboxHandler {
  void handle(SerializedEvent event);

  default OrderingPolicy orderingPolicy() {
    return OrderingPolicy.IDEMPOTENT;
  }
}
