package io.github.oxxultus.eventdock.inbox;

public record InboxProcessingResult(
    int claimed, int processed, int skipped, int retryScheduled, int failed) {
  public InboxProcessingResult {
    if (claimed < 0 || processed < 0 || skipped < 0 || retryScheduled < 0 || failed < 0) {
      throw new IllegalArgumentException("processing result counts must not be negative");
    }
    if (processed + skipped + retryScheduled + failed != claimed) {
      throw new IllegalArgumentException("inbox outcome counts must equal claimed count");
    }
  }

  public static InboxProcessingResult empty() {
    return new InboxProcessingResult(0, 0, 0, 0, 0);
  }

  InboxProcessingResult plus(Outcome outcome) {
    return switch (outcome) {
      case PROCESSED ->
          new InboxProcessingResult(claimed + 1, processed + 1, skipped, retryScheduled, failed);
      case SKIPPED ->
          new InboxProcessingResult(claimed + 1, processed, skipped + 1, retryScheduled, failed);
      case RETRY_SCHEDULED ->
          new InboxProcessingResult(claimed + 1, processed, skipped, retryScheduled + 1, failed);
      case FAILED ->
          new InboxProcessingResult(claimed + 1, processed, skipped, retryScheduled, failed + 1);
    };
  }

  enum Outcome {
    PROCESSED,
    SKIPPED,
    RETRY_SCHEDULED,
    FAILED
  }
}
