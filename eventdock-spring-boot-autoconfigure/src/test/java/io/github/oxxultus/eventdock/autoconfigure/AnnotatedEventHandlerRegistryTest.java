package io.github.oxxultus.eventdock.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.oxxultus.eventdock.core.EventHandler;
import io.github.oxxultus.eventdock.inbox.OrderingPolicy;
import java.util.List;
import org.junit.jupiter.api.Test;

class AnnotatedEventHandlerRegistryTest {
  @Test
  void resolvesAnnotatedHandlerByConsumerAndEventType() {
    EventHandler handler = new OrderCreatedHandler();
    var registry = new AnnotatedEventHandlerRegistry(List.of(handler));

    var resolved = registry.get("billing-service", "order.created");
    assertNotNull(resolved);
    assertEquals(OrderingPolicy.IDEMPOTENT, resolved.orderingPolicy());
    assertNull(registry.get("other-service", "order.created"));
  }

  @Test
  void supportsMultipleRoutesOnOneHandler() {
    EventHandler handler = new SharedHandler();
    var registry = new AnnotatedEventHandlerRegistry(List.of(handler));

    assertNotNull(registry.get("audit-service", "order.created"));
    assertEquals(
        OrderingPolicy.LATEST_WINS,
        registry.get("audit-service", "order.cancelled").orderingPolicy());
  }

  @Test
  void rejectsDuplicateRoutes() {
    assertThrows(
        IllegalStateException.class,
        () ->
            new AnnotatedEventHandlerRegistry(
                List.of(new OrderCreatedHandler(), new DuplicateOrderCreatedHandler())));
  }

  @EventDockHandler(consumerId = "billing-service", eventType = "order.created")
  static class OrderCreatedHandler implements EventHandler {
    @Override
    public void handle(io.github.oxxultus.eventdock.core.SerializedEvent event) {}
  }

  @EventDockHandler(consumerId = "billing-service", eventType = "order.created")
  static final class DuplicateOrderCreatedHandler extends OrderCreatedHandler {}

  @EventDockHandler(consumerId = "audit-service", eventType = "order.created")
  @EventDockHandler(
      consumerId = "audit-service",
      eventType = "order.cancelled",
      policy = OrderingPolicy.LATEST_WINS)
  static final class SharedHandler implements EventHandler {
    @Override
    public void handle(io.github.oxxultus.eventdock.core.SerializedEvent event) {}
  }
}
