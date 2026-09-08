package io.github.oxxultus.eventdock.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventEnvelope;
import io.github.oxxultus.eventdock.core.EventId;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class JacksonEventCodecTest {
  record Payload(long orderId) {}

  @Test
  void roundTripsPayloadAndEnvelopeMetadata() {
    var codec = new JacksonEventCodec(new ObjectMapper());
    var envelope =
        new EventEnvelope<>(
            new EventId("event-1"),
            "order.created",
            1,
            new AggregateRef("order", "42", 3),
            Instant.parse("2026-09-08T10:00:00Z"),
            new Payload(42),
            Map.of("trace-id", "trace-1"));

    var decoded = codec.decode(codec.encode(envelope), Payload.class);

    assertEquals(envelope, decoded);
  }
}
