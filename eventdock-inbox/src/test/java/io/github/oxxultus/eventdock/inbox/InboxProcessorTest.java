package io.github.oxxultus.eventdock.inbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.ExponentialBackoffRetryPolicy;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import io.github.oxxultus.eventdock.core.UnitOfWork;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InboxProcessorTest {
  private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

  @Test
  void processesIdempotentEvent() {
    var repository = new InMemoryInboxRepository(List.of(entry("event-1", 1, 0)));
    var handled = new ArrayList<EventId>();
    var processor = processor(repository, event -> handled.add(event.id()), new VersionRepository());

    var result = processor.process("billing", 10);

    assertEquals(new InboxProcessingResult(1, 1, 0, 0, 0), result);
    assertEquals(List.of(new EventId("event-1")), handled);
    assertEquals(List.of(new EventId("event-1")), repository.processed);
  }

  @Test
  void latestWinsSkipsOldEventAndAdvancesNewEvent() {
    var repository =
        new InMemoryInboxRepository(List.of(entry("old", 2, 0), entry("new", 4, 0)));
    var versions = new VersionRepository();
    versions.versions.put(new AggregateKey("billing", "order", "42"), 3L);
    var handled = new ArrayList<EventId>();
    InboxHandler handler =
        new InboxHandler() {
          public void handle(SerializedEvent event) {
            handled.add(event.id());
          }

          public OrderingPolicy orderingPolicy() {
            return OrderingPolicy.LATEST_WINS;
          }
        };
    var processor = processor(repository, handler, versions);

    var result = processor.process("billing", 10);

    assertEquals(new InboxProcessingResult(2, 1, 1, 0, 0), result);
    assertEquals(List.of(new EventId("new")), handled);
    assertEquals(List.of(new EventId("old")), repository.skipped);
    assertEquals(4L, versions.versions.get(new AggregateKey("billing", "order", "42")));
  }

  @Test
  void persistsFailureAndNotifiesWhenRetriesAreExhausted() {
    var repository = new InMemoryInboxRepository(List.of(entry("event-1", 1, 0)));
    var notified = new boolean[1];
    var processor =
        new InboxProcessor(
            repository,
            (consumerId, eventType) ->
                event -> {
                  throw new IllegalStateException("handler failed");
                },
            new VersionRepository(),
            policy(1),
            CLOCK,
            UnitOfWork.direct(),
            (entry, cause) -> {
              notified[0] = true;
              assertEquals(List.of(true), repository.exhaustedFailures);
            });

    var result = processor.process("billing", 10);

    assertEquals(new InboxProcessingResult(1, 0, 0, 0, 1), result);
    assertTrue(notified[0]);
  }

  private InboxProcessor processor(
      InMemoryInboxRepository repository,
      InboxHandler handler,
      AggregateVersionRepository aggregateVersions) {
    return new InboxProcessor(
        repository,
        (consumerId, eventType) -> handler,
        aggregateVersions,
        policy(3),
        CLOCK,
        UnitOfWork.direct(),
        InboxExhaustionHandler.noOp());
  }

  private static ExponentialBackoffRetryPolicy policy(int maxAttempts) {
    return new ExponentialBackoffRetryPolicy(
        maxAttempts, Duration.ofSeconds(1), Duration.ofMinutes(1), Duration.ofSeconds(30));
  }

  private static InboxEntry entry(String id, long version, int attemptCount) {
    return new InboxEntry(
        "billing",
        event(id, version),
        InboxStatus.PROCESSING,
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

  private static final class VersionRepository implements AggregateVersionRepository {
    private final Map<AggregateKey, Long> versions = new HashMap<>();

    public long getOrCreateAndLock(AggregateKey key, Instant now) {
      return versions.computeIfAbsent(key, ignored -> 0L);
    }

    public void advance(AggregateKey key, long version, Instant now) {
      versions.put(key, version);
    }
  }

  private static final class InMemoryInboxRepository implements InboxRepository {
    private final List<InboxEntry> entries;
    private final List<EventId> processed = new ArrayList<>();
    private final List<EventId> skipped = new ArrayList<>();
    private final List<Boolean> exhaustedFailures = new ArrayList<>();

    private InMemoryInboxRepository(List<InboxEntry> entries) {
      this.entries = entries;
    }

    public boolean receive(String consumerId, SerializedEvent event, Instant receivedAt) {
      return true;
    }

    public List<InboxEntry> claim(
        String consumerId, int limit, Instant now, Duration lockTimeout) {
      return entries.stream().limit(limit).toList();
    }

    public void markProcessed(String consumerId, EventId eventId, Instant processedAt) {
      processed.add(eventId);
    }

    public void markSkipped(
        String consumerId, EventId eventId, String reason, Instant processedAt) {
      skipped.add(eventId);
    }

    public void recordFailure(
        String consumerId,
        EventId eventId,
        String reason,
        Instant nextAttemptAt,
        boolean exhausted) {
      exhaustedFailures.add(exhausted);
    }
  }
}
