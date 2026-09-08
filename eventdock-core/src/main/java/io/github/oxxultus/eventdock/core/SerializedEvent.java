package io.github.oxxultus.eventdock.core;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record SerializedEvent(
    EventId id,
    String type,
    int schemaVersion,
    AggregateRef aggregate,
    Instant occurredAt,
    String contentType,
    byte[] payload,
    Map<String, String> metadata) {

  public SerializedEvent {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(aggregate, "aggregate must not be null");
    Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    Objects.requireNonNull(contentType, "contentType must not be null");
    Objects.requireNonNull(payload, "payload must not be null");
    if (type.isBlank() || contentType.isBlank()) {
      throw new IllegalArgumentException("type and contentType must not be blank");
    }
    if (schemaVersion < 1) {
      throw new IllegalArgumentException("schemaVersion must be positive");
    }
    payload = payload.clone();
    metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
  }

  @Override
  public byte[] payload() {
    return payload.clone();
  }
}
