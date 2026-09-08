package io.github.oxxultus.eventdock.core;

public interface EventCodec {
  SerializedEvent encode(EventEnvelope<?> event);

  <T> EventEnvelope<T> decode(SerializedEvent event, Class<T> payloadType);
}
