package io.github.oxxultus.eventdock.inbox;

import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.time.Instant;
import java.util.Objects;

public record InboxEntry(
    String consumerId,
    SerializedEvent event,
    InboxStatus status,
    int attemptCount,
    Instant availableAt,
    Instant lockedAt) {
  public InboxEntry {
    Objects.requireNonNull(consumerId, "consumerId must not be null");
    Objects.requireNonNull(event, "event must not be null");
    Objects.requireNonNull(status, "status must not be null");
    Objects.requireNonNull(availableAt, "availableAt must not be null");
    if (consumerId.isBlank()) {
      throw new IllegalArgumentException("consumerId must not be blank");
    }
    if (attemptCount < 0) {
      throw new IllegalArgumentException("attemptCount must not be negative");
    }
  }
}
