package io.github.oxxultus.eventdock.outbox;

public record OutboxProcessingResult(int claimed, int published, int retryScheduled, int failed) {
  public OutboxProcessingResult {
    if (claimed < 0 || published < 0 || retryScheduled < 0 || failed < 0) {
      throw new IllegalArgumentException("processing result counts must not be negative");
    }
    if (published + retryScheduled + failed != claimed) {
      throw new IllegalArgumentException("outbox outcome counts must equal claimed count");
    }
  }

  public static OutboxProcessingResult empty() {
    return new OutboxProcessingResult(0, 0, 0, 0);
  }

  OutboxProcessingResult plus(Outcome outcome) {
    return switch (outcome) {
      case PUBLISHED ->
          new OutboxProcessingResult(claimed + 1, published + 1, retryScheduled, failed);
      case RETRY_SCHEDULED ->
          new OutboxProcessingResult(claimed + 1, published, retryScheduled + 1, failed);
      case FAILED -> new OutboxProcessingResult(claimed + 1, published, retryScheduled, failed + 1);
    };
  }

  enum Outcome {
    PUBLISHED,
    RETRY_SCHEDULED,
    FAILED
  }
}
