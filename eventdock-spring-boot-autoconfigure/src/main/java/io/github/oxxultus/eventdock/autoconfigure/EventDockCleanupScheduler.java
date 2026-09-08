package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.inbox.InboxCleanupRepository;
import io.github.oxxultus.eventdock.outbox.OutboxCleanupRepository;
import java.time.Clock;
import org.springframework.scheduling.annotation.Scheduled;

public final class EventDockCleanupScheduler {
  private final OutboxCleanupRepository outbox;
  private final InboxCleanupRepository inbox;
  private final EventDockProperties properties;
  private final Clock clock;
  private final EventDockMetrics metrics;

  public EventDockCleanupScheduler(
      OutboxCleanupRepository outbox,
      InboxCleanupRepository inbox,
      EventDockProperties properties,
      Clock clock,
      EventDockMetrics metrics) {
    this.outbox = outbox;
    this.inbox = inbox;
    this.properties = properties;
    this.clock = clock;
    this.metrics = metrics;
  }

  @Scheduled(fixedDelayString = "${eventdock.cleanup.interval:1h}")
  public void cleanup() {
    var cleanup = properties.getCleanup();
    int outboxDeleted = outbox.deletePublishedBefore(
        clock.instant().minus(cleanup.getOutboxRetention()), cleanup.getBatchSize());
    int inboxDeleted = inbox.deleteCompletedBefore(
        clock.instant().minus(cleanup.getInboxRetention()), cleanup.getBatchSize());
    metrics.recordCleanup(outboxDeleted, inboxDeleted);
  }
}
