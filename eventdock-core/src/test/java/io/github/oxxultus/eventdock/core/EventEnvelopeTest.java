package io.github.oxxultus.eventdock.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EventEnvelopeTest {
  @Test
  void protectsMetadataFromExternalMutation() {
    var metadata = new java.util.HashMap<String, String>();
    metadata.put("correlationId", "c-1");

    var envelope =
        new EventEnvelope<>(
            EventId.random(),
            "order.created",
            1,
            new AggregateRef("order", "42", 1),
            Instant.EPOCH,
            Map.of("orderId", 42),
            metadata);

    metadata.put("correlationId", "changed");

    assertEquals("c-1", envelope.metadata().get("correlationId"));
  }

  @Test
  void rejectsNonPositiveSchemaVersion() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventEnvelope<>(
                EventId.random(),
                "order.created",
                0,
                new AggregateRef("order", "42", 1),
                Instant.EPOCH,
                "payload",
                Map.of()));
  }
}
