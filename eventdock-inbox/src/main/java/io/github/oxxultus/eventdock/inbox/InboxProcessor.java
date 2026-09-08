package io.github.oxxultus.eventdock.inbox;

import io.github.oxxultus.eventdock.core.UnitOfWork;
import io.github.oxxultus.eventdock.core.RetryPolicy;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

public final class InboxProcessor {
  private static final String OLDER_THAN_LAST_APPLIED = "OLDER_THAN_LAST_APPLIED";

  private final InboxRepository repository;
  private final InboxHandlerRegistry handlers;
  private final AggregateVersionRepository aggregateVersions;
  private final RetryPolicy retryPolicy;
  private final Clock clock;
  private final UnitOfWork unitOfWork;
  private final InboxExhaustionHandler exhaustionHandler;

  public InboxProcessor(
      InboxRepository repository,
      InboxHandlerRegistry handlers,
      AggregateVersionRepository aggregateVersions,
      RetryPolicy retryPolicy,
      Clock clock,
      UnitOfWork unitOfWork,
      InboxExhaustionHandler exhaustionHandler) {
    this.repository = Objects.requireNonNull(repository);
    this.handlers = Objects.requireNonNull(handlers);
    this.aggregateVersions = Objects.requireNonNull(aggregateVersions);
    this.retryPolicy = Objects.requireNonNull(retryPolicy);
    this.clock = Objects.requireNonNull(clock);
    this.unitOfWork = Objects.requireNonNull(unitOfWork);
    this.exhaustionHandler = Objects.requireNonNull(exhaustionHandler);
  }

  public InboxProcessingResult process(String consumerId, int batchSize) {
    if (consumerId == null || consumerId.isBlank()) {
      throw new IllegalArgumentException("consumerId must not be blank");
    }
    if (batchSize < 1) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    var entries = repository.claim(consumerId, batchSize, clock.instant(), retryPolicy.lockTimeout());
    InboxProcessingResult result = InboxProcessingResult.empty();
    for (InboxEntry entry : entries) {
      result = result.plus(process(entry));
    }
    return result;
  }

  private InboxProcessingResult.Outcome process(InboxEntry entry) {
    try {
      return unitOfWork.execute(() -> processClaimed(entry));
    } catch (RuntimeException exception) {
      int nextAttempt = entry.attemptCount() + 1;
      boolean exhausted = retryPolicy.exhausted(nextAttempt);
      Instant nextAttemptAt = clock.instant().plus(retryPolicy.delayAfter(nextAttempt));
      unitOfWork.execute(
          () ->
              repository.recordFailure(
                  entry.consumerId(),
                  entry.event().id(),
                  safeMessage(exception),
                  nextAttemptAt,
                  exhausted));
      if (exhausted) {
        notifyExhausted(entry, exception);
        return InboxProcessingResult.Outcome.FAILED;
      }
      return InboxProcessingResult.Outcome.RETRY_SCHEDULED;
    }
  }

  private InboxProcessingResult.Outcome processClaimed(InboxEntry entry) {
    requireProcessing(entry);
    InboxHandler handler = handlers.get(entry.consumerId(), entry.event().type());
    if (handler == null) {
      throw new IllegalStateException(
          "no inbox handler for consumerId="
              + entry.consumerId()
              + ", eventType="
              + entry.event().type());
    }
    return switch (handler.orderingPolicy()) {
      case IDEMPOTENT -> processIdempotent(entry, handler);
      case LATEST_WINS -> processLatestWins(entry, handler);
    };
  }

  private InboxProcessingResult.Outcome processIdempotent(
      InboxEntry entry, InboxHandler handler) {
    handler.handle(entry.event());
    repository.markProcessed(entry.consumerId(), entry.event().id(), clock.instant());
    return InboxProcessingResult.Outcome.PROCESSED;
  }

  private InboxProcessingResult.Outcome processLatestWins(
      InboxEntry entry, InboxHandler handler) {
    var aggregate = entry.event().aggregate();
    var key = new AggregateKey(entry.consumerId(), aggregate.type(), aggregate.id());
    Instant now = clock.instant();
    long lastProcessed = aggregateVersions.getOrCreateAndLock(key, now);
    if (aggregate.version() <= lastProcessed) {
      repository.markSkipped(entry.consumerId(), entry.event().id(), OLDER_THAN_LAST_APPLIED, now);
      return InboxProcessingResult.Outcome.SKIPPED;
    }
    handler.handle(entry.event());
    aggregateVersions.advance(key, aggregate.version(), now);
    repository.markProcessed(entry.consumerId(), entry.event().id(), now);
    return InboxProcessingResult.Outcome.PROCESSED;
  }

  private void requireProcessing(InboxEntry entry) {
    if (entry.status() != InboxStatus.PROCESSING) {
      throw new IllegalStateException(
          "only PROCESSING inbox entries can be handled: " + entry.event().id().value());
    }
  }

  private void notifyExhausted(InboxEntry entry, RuntimeException cause) {
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
