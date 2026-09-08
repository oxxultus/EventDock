package io.github.oxxultus.eventdock.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OutboxProcessorTest {
  @Test
  void publishesClaimedEntriesAndMarksThemPublished() {
    var repository = new InMemoryRepository();
    var event =
        new SerializedEvent(
            new EventId("event-1"),
            "order.created",
            1,
            new AggregateRef("order", "42", 1),
            Instant.EPOCH,
            "application/json",
            new byte[] {1},
            Map.of());
    repository.entries.add(new OutboxEntry(event, OutboxStatus.PENDING, 0, Instant.EPOCH, null));
    var processor =
        new OutboxProcessor(
            repository,
            ignored -> {},
            new FixedRetryPolicy(),
            Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));

    assertEquals(1, processor.process(10));
    assertEquals(List.of(event.id()), repository.published);
  }

  private static final class FixedRetryPolicy implements RetryPolicy {
    public Duration lockTimeout() {
      return Duration.ofSeconds(30);
    }

    public boolean exhausted(int attemptCount) {
      return attemptCount >= 3;
    }

    public Duration delayAfter(int attemptCount) {
      return Duration.ofSeconds(1);
    }
  }

  private static final class InMemoryRepository implements OutboxRepository {
    private final List<OutboxEntry> entries = new ArrayList<>();
    private final List<EventId> published = new ArrayList<>();

    public void append(SerializedEvent event) {}

    public List<OutboxEntry> claim(int limit, Instant now, Duration lockTimeout) {
      return entries.stream().limit(limit).toList();
    }

    public void markPublished(EventId eventId, Instant publishedAt) {
      published.add(eventId);
    }

    public void recordFailure(
        EventId eventId, String reason, Instant nextAttemptAt, boolean exhausted) {}
  }
}
