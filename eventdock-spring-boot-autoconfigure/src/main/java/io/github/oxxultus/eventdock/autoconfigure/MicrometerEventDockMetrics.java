package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.inbox.InboxProcessingResult;
import io.github.oxxultus.eventdock.outbox.OutboxProcessingResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

public final class MicrometerEventDockMetrics implements EventDockMetrics {
  private final MeterRegistry registry;

  public MicrometerEventDockMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  @Override
  public void recordOutbox(OutboxProcessingResult result) {
    increment("eventdock.outbox.events", "published", result.published());
    increment("eventdock.outbox.events", "retry_scheduled", result.retryScheduled());
    increment("eventdock.outbox.events", "failed", result.failed());
  }

  @Override
  public void recordInbox(InboxProcessingResult result) {
    increment("eventdock.inbox.events", "processed", result.processed());
    increment("eventdock.inbox.events", "skipped", result.skipped());
    increment("eventdock.inbox.events", "retry_scheduled", result.retryScheduled());
    increment("eventdock.inbox.events", "failed", result.failed());
  }

  @Override
  public void recordCleanup(int outboxDeleted, int inboxDeleted) {
    increment("eventdock.cleanup.events", "outbox", outboxDeleted);
    increment("eventdock.cleanup.events", "inbox", inboxDeleted);
  }

  private void increment(String name, String outcome, int amount) {
    if (amount > 0) {
      Counter.builder(name).tag("outcome", outcome).register(registry).increment(amount);
    }
  }
}
