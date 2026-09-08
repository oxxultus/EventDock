package io.github.oxxultus.eventdock.core;

import java.util.Objects;

public final class DirectEventProcessor {
  private final EventHandlerRegistry handlers;
  private final UnitOfWork unitOfWork;

  public DirectEventProcessor(EventHandlerRegistry handlers, UnitOfWork unitOfWork) {
    this.handlers = Objects.requireNonNull(handlers);
    this.unitOfWork = Objects.requireNonNull(unitOfWork);
  }

  public void process(String consumerId, SerializedEvent event) {
    if (consumerId == null || consumerId.isBlank()) {
      throw new IllegalArgumentException("consumerId must not be blank");
    }
    Objects.requireNonNull(event, "event must not be null");
    EventHandler handler = handlers.get(consumerId, event.type());
    if (handler == null) {
      throw new IllegalStateException(
          "no event handler for consumerId=" + consumerId + ", eventType=" + event.type());
    }
    unitOfWork.execute(() -> handler.handle(event));
  }
}
