package io.github.oxxultus.eventdock.core;

import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DirectEventWriterTest {
  @Test
  void encodesAndPublishesWithoutPersistence() {
    SerializedEvent serialized = serializedEvent();
    AtomicReference<SerializedEvent> published = new AtomicReference<>();
    EventCodec codec = new FixedCodec(serialized);
    var writer = new DirectEventWriter(codec, published::set);

    writer.write(envelope());

    assertSame(serialized, published.get());
  }

  private EventEnvelope<String> envelope() {
    return new EventEnvelope<>(
        new EventId("event-1"),
        "order.created",
        1,
        new AggregateRef("order", "1", 1),
        Instant.EPOCH,
        "payload",
        Map.of());
  }

  private SerializedEvent serializedEvent() {
    return new SerializedEvent(
        new EventId("event-1"),
        "order.created",
        1,
        new AggregateRef("order", "1", 1),
        Instant.EPOCH,
        "application/json",
        new byte[] {1},
        Map.of());
  }

  private record FixedCodec(SerializedEvent encoded) implements EventCodec {
    @Override
    public SerializedEvent encode(EventEnvelope<?> event) {
      return encoded;
    }

    @Override
    public <T> EventEnvelope<T> decode(SerializedEvent event, Class<T> payloadType) {
      throw new UnsupportedOperationException();
    }
  }
}
