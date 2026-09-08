package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.inbox.InboxProcessor;
import org.springframework.scheduling.annotation.Scheduled;

public final class EventDockInboxScheduler {
  private final InboxProcessor processor;
  private final EventDockProperties properties;
  private final EventDockMetrics metrics;

  public EventDockInboxScheduler(
      InboxProcessor processor, EventDockProperties properties, EventDockMetrics metrics) {
    this.processor = processor;
    this.properties = properties;
    this.metrics = metrics;
  }

  @Scheduled(fixedDelayString = "${eventdock.inbox.poll-interval:1s}")
  public void process() {
    metrics.recordInbox(
        processor.process(
            properties.getInbox().getConsumerId(), properties.getInbox().getBatchSize()));
  }
}
