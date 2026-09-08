package io.github.oxxultus.eventdock.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.EventPublisher;
import io.github.oxxultus.eventdock.core.ExponentialBackoffRetryPolicy;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import io.github.oxxultus.eventdock.core.UnitOfWork;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OutboxProcessorTest {
  private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

  @Test
  void publishesClaimedEntriesAndReportsOutcome() {
    var repository = new InMemoryRepository(List.of(entry("event-1", 0)));
    var processor = processor(repository, ignored -> {}, policy(3), OutboxExhaustionHandler.noOp());

    var result = processor.process(10);

    assertEquals(new OutboxProcessingResult(1, 1, 0, 0), result);
    assertEquals(List.of(new EventId("event-1")), repository.published);
  }

  @Test
  void schedulesRetryAndContinuesRemainingBatch() {
    var repository = new InMemoryRepository(List.of(entry("fails", 0), entry("succeeds", 0)));
    var processor =
        processor(
            repository,
            event -> {
              if (event.id().value().equals("fails")) {
                throw new IllegalStateException("broker unavailable");
              }
            },
            policy(3),
            OutboxExhaustionHandler.noOp());

    var result = processor.process(10);

    assertEquals(new OutboxProcessingResult(2, 1, 1, 0), result);
    assertEquals(List.of(new EventId("succeeds")), repository.published);
    assertEquals(List.of(false), repository.exhaustedFailures);
  }

  @Test
  void persistsExhaustionBeforeInvokingCallback() {
    var repository = new InMemoryRepository(List.of(entry("event-1", 0)));
    var notified = new boolean[1];
    var processor =
        processor(
            repository,
            ignored -> {
              throw new IllegalStateException("broker unavailable");
            },
            policy(1),
            (entry, cause) -> {
              notified[0] = true;
              assertEquals(List.of(true), repository.exhaustedFailures);
              throw new IllegalStateException("notification failure must be isolated");
            });

    var result = processor.process(10);

    assertEquals(new OutboxProcessingResult(1, 0, 0, 1), result);
    assertTrue(notified[0]);
  }

  private OutboxProcessor processor(
      InMemoryRepository repository,
      EventPublisher publisher,
      ExponentialBackoffRetryPolicy retryPolicy,
      OutboxExhaustionHandler exhaustionHandler) {
    return new OutboxProcessor(
        repository, publisher, retryPolicy, CLOCK, UnitOfWork.direct(), exhaustionHandler);
  }

  private static ExponentialBackoffRetryPolicy policy(int maxAttempts) {
    return new ExponentialBackoffRetryPolicy(
        maxAttempts, Duration.ofSeconds(1), Duration.ofMinutes(1), Duration.ofSeconds(30));
  }

  private static OutboxEntry entry(String id, int attemptCount) {
    return new OutboxEntry(
        event(id, attemptCount + 1),
        OutboxStatus.PROCESSING,
        attemptCount,
        Instant.EPOCH,
        Instant.EPOCH);
  }

  private static SerializedEvent event(String id, long version) {
    return new SerializedEvent(
        new EventId(id),
        "order.created",
        1,
        new AggregateRef("order", "42", version),
        Instant.EPOCH,
        "application/json",
        new byte[] {1},
        Map.of());
  }

  private static final class InMemoryRepository implements OutboxRepository {
    private final List<OutboxEntry> entries;
    private final List<EventId> published = new ArrayList<>();
    private final List<Boolean> exhaustedFailures = new ArrayList<>();

    private InMemoryRepository(List<OutboxEntry> entries) {
      this.entries = entries;
    }

    public void append(SerializedEvent event) {}

    public List<OutboxEntry> claim(int limit, Instant now, Duration lockTimeout) {
      return entries.stream().limit(limit).toList();
    }

    public void markPublished(EventId eventId, Instant publishedAt) {
      published.add(eventId);
    }

    public void recordFailure(
        EventId eventId, String reason, Instant nextAttemptAt, boolean exhausted) {
      exhaustedFailures.add(exhausted);
    }
  }
}
