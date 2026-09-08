package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.outbox.OutboxProcessor;
import org.springframework.scheduling.annotation.Scheduled;

public final class EventDockOutboxScheduler {
  private final OutboxProcessor processor;
  private final EventDockProperties properties;
  private final EventDockMetrics metrics;

  public EventDockOutboxScheduler(
      OutboxProcessor processor, EventDockProperties properties, EventDockMetrics metrics) {
    this.processor = processor;
    this.properties = properties;
    this.metrics = metrics;
  }

  @Scheduled(fixedDelayString = "${eventdock.outbox.poll-interval:1s}")
  public void process() {
    metrics.recordOutbox(processor.process(properties.getOutbox().getBatchSize()));
  }
}
