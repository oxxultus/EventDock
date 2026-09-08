package io.github.oxxultus.eventdock.outbox;

import io.github.oxxultus.eventdock.core.EventPublisher;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

public final class OutboxProcessor {
  private final OutboxRepository repository;
  private final EventPublisher publisher;
  private final RetryPolicy retryPolicy;
  private final Clock clock;

  public OutboxProcessor(
      OutboxRepository repository,
      EventPublisher publisher,
      RetryPolicy retryPolicy,
      Clock clock) {
    this.repository = Objects.requireNonNull(repository);
    this.publisher = Objects.requireNonNull(publisher);
    this.retryPolicy = Objects.requireNonNull(retryPolicy);
    this.clock = Objects.requireNonNull(clock);
  }

  public int process(int batchSize) {
    Instant now = clock.instant();
    var entries = repository.claim(batchSize, now, retryPolicy.lockTimeout());
    entries.forEach(this::publish);
    return entries.size();
  }

  private void publish(OutboxEntry entry) {
    try {
      publisher.publish(entry.event());
      repository.markPublished(entry.event().id(), clock.instant());
    } catch (RuntimeException exception) {
      int nextAttempt = entry.attemptCount() + 1;
      boolean exhausted = retryPolicy.exhausted(nextAttempt);
      Instant nextAttemptAt = clock.instant().plus(retryPolicy.delayAfter(nextAttempt));
      repository.recordFailure(
          entry.event().id(), safeMessage(exception), nextAttemptAt, exhausted);
    }
  }

  private String safeMessage(RuntimeException exception) {
    return exception.getMessage() == null ? exception.getClass().getName() : exception.getMessage();
  }
}
