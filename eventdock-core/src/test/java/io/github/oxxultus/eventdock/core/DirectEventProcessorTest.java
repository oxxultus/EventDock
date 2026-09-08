package io.github.oxxultus.eventdock.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class DirectEventProcessorTest {
  @Test
  void resolvesHandlerAndRunsItInUnitOfWork() {
    SerializedEvent event = event();
    AtomicReference<SerializedEvent> handled = new AtomicReference<>();
    AtomicInteger transactions = new AtomicInteger();
    EventHandlerRegistry registry = (consumerId, eventType) -> handled::set;
    UnitOfWork unitOfWork =
        new UnitOfWork() {
          @Override
          public <T> T execute(Supplier<T> action) {
            transactions.incrementAndGet();
            return action.get();
          }
        };

    new DirectEventProcessor(registry, unitOfWork).process("billing", event);

    assertSame(event, handled.get());
    assertEquals(1, transactions.get());
  }

  @Test
  void rejectsMissingHandler() {
    var processor = new DirectEventProcessor((consumerId, eventType) -> null, UnitOfWork.direct());

    assertThrows(IllegalStateException.class, () -> processor.process("billing", event()));
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
