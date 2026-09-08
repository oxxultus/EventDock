package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.inbox.InboxProcessor;
import org.springframework.scheduling.annotation.Scheduled;

public final class EventDockInboxScheduler {
  private final InboxProcessor processor;
  private final EventDockProperties properties;

  public EventDockInboxScheduler(InboxProcessor processor, EventDockProperties properties) {
    this.processor = processor;
    this.properties = properties;
  }

  @Scheduled(fixedDelayString = "${eventdock.inbox.poll-interval:1s}")
  public void process() {
    processor.process(properties.getInbox().getConsumerId(), properties.getInbox().getBatchSize());
  }
}
