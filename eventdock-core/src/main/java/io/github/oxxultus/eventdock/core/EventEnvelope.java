package io.github.oxxultus.eventdock.core;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record EventEnvelope<T>(
    EventId id,
    String type,
    int schemaVersion,
    AggregateRef aggregate,
    Instant occurredAt,
    T payload,
    Map<String, String> metadata) {

  public EventEnvelope {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(aggregate, "aggregate must not be null");
    Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    Objects.requireNonNull(payload, "payload must not be null");
    if (type.isBlank()) {
      throw new IllegalArgumentException("type must not be blank");
    }
    if (schemaVersion < 1) {
      throw new IllegalArgumentException("schemaVersion must be positive");
    }
    metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
  }
}
