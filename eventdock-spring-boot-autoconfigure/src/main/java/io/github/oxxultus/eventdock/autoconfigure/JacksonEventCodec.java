package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.core.EventCodec;
import io.github.oxxultus.eventdock.core.EventEnvelope;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

public final class JacksonEventCodec implements EventCodec {
  private final ObjectMapper objectMapper;

  public JacksonEventCodec(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public SerializedEvent encode(EventEnvelope<?> event) {
    try {
      return new SerializedEvent(event.id(), event.type(), event.schemaVersion(), event.aggregate(),
          event.occurredAt(), "application/json", objectMapper.writeValueAsBytes(event.payload()),
          event.metadata());
    } catch (JacksonException exception) {
      throw new IllegalArgumentException("failed to serialize event payload", exception);
    }
  }

  @Override
  public <T> EventEnvelope<T> decode(SerializedEvent event, Class<T> payloadType) {
    try {
      return new EventEnvelope<>(event.id(), event.type(), event.schemaVersion(), event.aggregate(),
          event.occurredAt(), objectMapper.readValue(event.payload(), payloadType), event.metadata());
    } catch (JacksonException exception) {
      throw new IllegalArgumentException("failed to deserialize event payload", exception);
    }
  }
}
