package io.github.oxxultus.eventdock.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class EventDockCleanupSchedulerTest {
  @Test
  void appliesIndependentRetentionBoundaries() {
    var properties = new EventDockProperties();
    var outboxBoundary = new AtomicReference<Instant>();
    var inboxBoundary = new AtomicReference<Instant>();
    var now = Instant.parse("2026-09-08T12:00:00Z");
    var scheduler =
        new EventDockCleanupScheduler(
            (before, limit) -> {
              outboxBoundary.set(before);
              return 2;
            },
            (before, limit) -> {
              inboxBoundary.set(before);
              return 3;
            },
            properties,
            Clock.fixed(now, ZoneOffset.UTC),
            EventDockMetrics.noOp());

    scheduler.cleanup();

    assertEquals(now.minus(properties.getCleanup().getOutboxRetention()), outboxBoundary.get());
    assertEquals(now.minus(properties.getCleanup().getInboxRetention()), inboxBoundary.get());
  }
}
