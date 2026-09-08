package io.github.oxxultus.eventdock.core;

import java.time.Instant;
import java.util.Map;

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
    payload = payload.clone();
    metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
  }

  @Override
  public byte[] payload() {
    return payload.clone();
  }
}
