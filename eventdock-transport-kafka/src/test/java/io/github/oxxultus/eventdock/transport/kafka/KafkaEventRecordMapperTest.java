package io.github.oxxultus.eventdock.transport.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class KafkaEventRecordMapperTest {
  private final KafkaEventRecordMapper mapper =
      new KafkaEventRecordMapper(KafkaTopicResolver.eventType());

  @Test
  void roundTripsEventThroughKafkaRecord() {
    var event = event("order.created");
    var produced = mapper.toProducerRecord(event);
    var consumed =
        new ConsumerRecord<>(
            produced.topic(),
            0,
            3,
            0,
            TimestampType.CREATE_TIME,
            0,
            0,
            produced.key(),
            produced.value(),
            produced.headers(),
            Optional.empty());

    var decoded = mapper.fromConsumerRecord(consumed);

    assertEquals(event.id(), decoded.id());
    assertEquals(event.aggregate(), decoded.aggregate());
    assertEquals(event.metadata(), decoded.metadata());
    assertArrayEquals(event.payload(), decoded.payload());
    assertEquals("order.created", produced.topic());
    assertEquals("order:42", produced.key());
  }

  @Test
  void rejectsInvalidTopic() {
    assertThrows(
        IllegalArgumentException.class, () -> mapper.toProducerRecord(event("invalid topic")));
  }

  private SerializedEvent event(String type) {
    return new SerializedEvent(
        new EventId("event-1"),
        type,
        1,
        new AggregateRef("order", "42", 7),
        Instant.parse("2026-09-08T09:00:00Z"),
        "application/json",
        "{}".getBytes(StandardCharsets.UTF_8),
        Map.of("correlation-id", "trace-1"));
  }
}
