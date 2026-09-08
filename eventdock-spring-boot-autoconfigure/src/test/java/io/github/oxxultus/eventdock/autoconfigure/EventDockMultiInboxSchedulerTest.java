package io.github.oxxultus.eventdock.autoconfigure;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.oxxultus.eventdock.core.ConsumerMode;
import io.github.oxxultus.eventdock.inbox.InboxProcessingResult;
import io.github.oxxultus.eventdock.inbox.InboxProcessor;
import java.util.List;
import org.junit.jupiter.api.Test;

class EventDockMultiInboxSchedulerTest {
  @Test
  void processesDefaultAndRouteSpecificInboxConsumerIds() {
    var properties = new EventDockProperties();
    var binding = new EventDockProperties.ConsumerBinding();
    binding.setId("member-events");
    binding.setMode(ConsumerMode.INBOX);
    binding.setTopics(List.of("member.created", "member.updated"));
    var created = route("member.created", "member-created");
    var updated = route("member.updated", "member-updated");
    binding.setRoutes(List.of(created, updated));
    properties.setConsumers(List.of(binding));
    var processor = mock(InboxProcessor.class);
    when(processor.process("member-events", 100)).thenReturn(InboxProcessingResult.empty());
    when(processor.process("member-created", 100)).thenReturn(InboxProcessingResult.empty());
    when(processor.process("member-updated", 100)).thenReturn(InboxProcessingResult.empty());

    new EventDockMultiInboxScheduler(processor, properties, EventDockMetrics.noOp()).process();

    verify(processor).process("member-events", 100);
    verify(processor).process("member-created", 100);
    verify(processor).process("member-updated", 100);
  }

  private EventDockProperties.Route route(String eventType, String consumerId) {
    var route = new EventDockProperties.Route();
    route.setEventType(eventType);
    route.setConsumerId(consumerId);
    return route;
  }
}
