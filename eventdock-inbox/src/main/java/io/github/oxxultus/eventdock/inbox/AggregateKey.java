package io.github.oxxultus.eventdock.inbox;

import java.util.Objects;

public record AggregateKey(String consumerId, String aggregateType, String aggregateId) {
  public AggregateKey {
    Objects.requireNonNull(consumerId, "consumerId must not be null");
    Objects.requireNonNull(aggregateType, "aggregateType must not be null");
    Objects.requireNonNull(aggregateId, "aggregateId must not be null");
    if (consumerId.isBlank() || aggregateType.isBlank() || aggregateId.isBlank()) {
      throw new IllegalArgumentException(
          "consumerId, aggregateType, and aggregateId must not be blank");
    }
  }
}
