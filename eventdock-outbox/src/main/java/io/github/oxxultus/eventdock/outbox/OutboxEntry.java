package io.github.oxxultus.eventdock.outbox;

import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.time.Instant;
import java.util.Objects;

public record OutboxEntry(
    SerializedEvent event,
    OutboxStatus status,
    int attemptCount,
    Instant availableAt,
    Instant lockedAt) {
  public OutboxEntry {
    Objects.requireNonNull(event, "event must not be null");
    Objects.requireNonNull(status, "status must not be null");
    Objects.requireNonNull(availableAt, "availableAt must not be null");
    if (attemptCount < 0) {
      throw new IllegalArgumentException("attemptCount must not be negative");
    }
  }
}
