package io.github.oxxultus.eventdock.outbox;

import io.github.oxxultus.eventdock.core.EventPublisher;
import io.github.oxxultus.eventdock.core.RetryPolicy;
import io.github.oxxultus.eventdock.core.UnitOfWork;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

public final class OutboxProcessor {
  private final OutboxRepository repository;
  private final EventPublisher publisher;
  private final RetryPolicy retryPolicy;
  private final Clock clock;
  private final UnitOfWork unitOfWork;
  private final OutboxExhaustionHandler exhaustionHandler;

  public OutboxProcessor(
      OutboxRepository repository,
      EventPublisher publisher,
      RetryPolicy retryPolicy,
      Clock clock,
      UnitOfWork unitOfWork,
      OutboxExhaustionHandler exhaustionHandler) {
    this.repository = Objects.requireNonNull(repository);
    this.publisher = Objects.requireNonNull(publisher);
    this.retryPolicy = Objects.requireNonNull(retryPolicy);
    this.clock = Objects.requireNonNull(clock);
    this.unitOfWork = Objects.requireNonNull(unitOfWork);
    this.exhaustionHandler = Objects.requireNonNull(exhaustionHandler);
  }

  public OutboxProcessingResult process(int batchSize) {
    if (batchSize < 1) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    Instant now = clock.instant();
    var entries = repository.claim(batchSize, now, retryPolicy.lockTimeout());
    OutboxProcessingResult result = OutboxProcessingResult.empty();
    for (OutboxEntry entry : entries) {
      result = result.plus(publish(entry));
    }
    return result;
  }

  private OutboxProcessingResult.Outcome publish(OutboxEntry entry) {
    try {
      unitOfWork.execute(
          () -> {
            requireProcessing(entry);
            publisher.publish(entry.event());
            repository.markPublished(entry.event().id(), clock.instant());
          });
      return OutboxProcessingResult.Outcome.PUBLISHED;
    } catch (RuntimeException exception) {
      int nextAttempt = entry.attemptCount() + 1;
      boolean exhausted = retryPolicy.exhausted(nextAttempt);
      Instant nextAttemptAt = clock.instant().plus(retryPolicy.delayAfter(nextAttempt));
      unitOfWork.execute(
          () ->
              repository.recordFailure(
                  entry.event().id(), safeMessage(exception), nextAttemptAt, exhausted));
      if (exhausted) {
        notifyExhausted(entry, exception);
        return OutboxProcessingResult.Outcome.FAILED;
      }
      return OutboxProcessingResult.Outcome.RETRY_SCHEDULED;
    }
  }

  private void requireProcessing(OutboxEntry entry) {
    if (entry.status() != OutboxStatus.PROCESSING) {
      throw new IllegalStateException(
          "only PROCESSING outbox entries can be published: " + entry.event().id().value());
    }
  }

  private void notifyExhausted(OutboxEntry entry, RuntimeException cause) {
    try {
      exhaustionHandler.onExhausted(entry, cause);
    } catch (RuntimeException ignored) {
      // Failure persistence is authoritative; an operational callback must not undo it.
    }
  }

  private String safeMessage(RuntimeException exception) {
    String message =
        exception.getMessage() == null ? exception.getClass().getName() : exception.getMessage();
    return message.length() <= 1000 ? message : message.substring(0, 1000);
  }
}
