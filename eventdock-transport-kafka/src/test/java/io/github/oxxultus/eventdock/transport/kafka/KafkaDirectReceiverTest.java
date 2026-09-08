package io.github.oxxultus.eventdock.transport.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.DirectEventProcessor;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import io.github.oxxultus.eventdock.core.UnitOfWork;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class KafkaDirectReceiverTest {
  @Test
  void mapsKafkaRecordAndInvokesHandlerDirectly() {
    var mapper = new KafkaEventRecordMapper(KafkaTopicResolver.eventType());
    SerializedEvent event = event();
    var produced = mapper.toProducerRecord(event);
    var consumed =
        new ConsumerRecord<>(
            produced.topic(),
            0,
            1,
            0,
            TimestampType.CREATE_TIME,
            0,
            0,
            produced.key(),
            produced.value(),
            produced.headers(),
            Optional.empty());
    AtomicReference<SerializedEvent> handled = new AtomicReference<>();
    var processor =
        new DirectEventProcessor(
            (consumerId, eventType) -> {
              assertEquals("billing", consumerId);
              return handled::set;
            },
            UnitOfWork.direct());

    new KafkaDirectReceiver("billing", mapper, processor).receive(consumed);

    assertEquals(event.id(), handled.get().id());
    assertEquals(event.type(), handled.get().type());
  }

  private SerializedEvent event() {
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
}
