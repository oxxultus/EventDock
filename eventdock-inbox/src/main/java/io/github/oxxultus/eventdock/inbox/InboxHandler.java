package io.github.oxxultus.eventdock.inbox;

import io.github.oxxultus.eventdock.core.SerializedEvent;
import io.github.oxxultus.eventdock.core.EventHandler;

@FunctionalInterface
public interface InboxHandler extends EventHandler {
  @Override
  void handle(SerializedEvent event);

  default OrderingPolicy orderingPolicy() {
    return OrderingPolicy.IDEMPOTENT;
  }
}
