package io.github.oxxultus.eventdock.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.oxxultus.eventdock.core.ConsumerMode;
import java.util.List;
import org.junit.jupiter.api.Test;

class EventDockPropertiesTest {
  @Test
  void producerOnlyDefaultsAreValid() {
    assertDoesNotThrow(new EventDockProperties()::validate);
  }

  @Test
  void enabledInboxRequiresIdentityAndTopics() {
    var properties = new EventDockProperties();
    properties.getInbox().setEnabled(true);

    assertThrows(IllegalStateException.class, properties::validate);
  }

  @Test
  void validatesMultipleBindingsAndRouteOverrides() {
    var properties = new EventDockProperties();
    var binding = new EventDockProperties.ConsumerBinding();
    binding.setId("member-events");
    binding.setMode(ConsumerMode.INBOX);
    binding.setTopics(List.of("member.created"));
    binding.getKafka().setGroupId("core-order-member-events");
    var route = new EventDockProperties.Route();
    route.setEventType("member.created");
    route.setConsumerId("core-order-member-created");
    binding.setRoutes(List.of(route));
    properties.setConsumers(List.of(binding));

    assertDoesNotThrow(properties::validate);
    org.junit.jupiter.api.Assertions.assertEquals(
        "core-order-member-created", binding.consumerId("member.created"));
  }

  @Test
  void rejectsMixingLegacyAndMultipleConsumerSettings() {
    var properties = new EventDockProperties();
    properties.getInbox().setConsumerId("legacy");
    properties.getInbox().setTopics(List.of("order.created"));
    var binding = new EventDockProperties.ConsumerBinding();
    binding.setId("orders");
    binding.setTopics(List.of("order.created"));
    properties.setConsumers(List.of(binding));

    assertThrows(IllegalStateException.class, properties::validate);
  }
}
