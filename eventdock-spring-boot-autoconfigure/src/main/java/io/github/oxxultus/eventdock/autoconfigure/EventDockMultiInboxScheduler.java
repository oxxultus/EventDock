package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.core.ConsumerMode;
import io.github.oxxultus.eventdock.inbox.InboxProcessor;
import java.util.LinkedHashSet;
import org.springframework.scheduling.annotation.Scheduled;

public final class EventDockMultiInboxScheduler {
  private final InboxProcessor processor;
  private final EventDockProperties properties;
  private final EventDockMetrics metrics;

  public EventDockMultiInboxScheduler(
      InboxProcessor processor, EventDockProperties properties, EventDockMetrics metrics) {
    this.processor = processor;
    this.properties = properties;
    this.metrics = metrics;
  }

  @Scheduled(fixedDelayString = "${eventdock.inbox.poll-interval:1s}")
  public void process() {
    var consumerIds = new LinkedHashSet<String>();
    properties.getConsumers().stream()
        .filter(binding -> binding.getMode() == ConsumerMode.INBOX)
        .forEach(
            binding -> {
              consumerIds.add(binding.getId());
              binding.getRoutes().forEach(route -> consumerIds.add(route.getConsumerId()));
            });
    consumerIds.forEach(
        consumerId ->
            metrics.recordInbox(
                processor.process(consumerId, properties.getInbox().getBatchSize())));
  }
}
