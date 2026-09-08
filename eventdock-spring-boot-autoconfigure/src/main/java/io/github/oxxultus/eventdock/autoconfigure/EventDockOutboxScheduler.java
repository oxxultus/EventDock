package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.outbox.OutboxProcessor;
import org.springframework.scheduling.annotation.Scheduled;

public final class EventDockOutboxScheduler {
  private final OutboxProcessor processor;
  private final EventDockProperties properties;

  public EventDockOutboxScheduler(OutboxProcessor processor, EventDockProperties properties) {
    this.processor = processor;
    this.properties = properties;
  }

  @Scheduled(fixedDelayString = "${eventdock.outbox.poll-interval:1s}")
  public void process() {
    processor.process(properties.getOutbox().getBatchSize());
  }
}
