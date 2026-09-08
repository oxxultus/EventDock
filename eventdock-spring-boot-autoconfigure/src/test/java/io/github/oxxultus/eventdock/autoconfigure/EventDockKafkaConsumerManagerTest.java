package io.github.oxxultus.eventdock.autoconfigure;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.ConsumerMode;
import io.github.oxxultus.eventdock.core.DirectEventProcessor;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import io.github.oxxultus.eventdock.inbox.InboxProcessor;
import io.github.oxxultus.eventdock.inbox.InboxRepository;
import io.github.oxxultus.eventdock.transport.kafka.KafkaEventRecordMapper;
import io.github.oxxultus.eventdock.transport.kafka.KafkaTopicResolver;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.junit.jupiter.api.Test;

class EventDockKafkaConsumerManagerTest {
  @Test
  @SuppressWarnings("unchecked")
  void createsIndependentGroupsAndRoutesEachEventToItsConsumerId() {
    var inboxBinding = binding("member-events", ConsumerMode.INBOX, "member-group", "member.created", "member-created");
    var directBinding = binding("notifications", ConsumerMode.DIRECT, "notification-group", "order.created", null);
    var inboxContainer = mock(ConcurrentMessageListenerContainer.class);
    var directContainer = mock(ConcurrentMessageListenerContainer.class);
    var inboxProperties = new ContainerProperties("unused");
    var directProperties = new ContainerProperties("unused");
    when(inboxContainer.getContainerProperties()).thenReturn(inboxProperties);
    when(directContainer.getContainerProperties()).thenReturn(directProperties);
    var factory = mock(ConcurrentKafkaListenerContainerFactory.class);
    when(factory.createContainer(any(String[].class))).thenReturn(inboxContainer, directContainer);
    var repository = mock(InboxRepository.class);
    var directProcessor = mock(DirectEventProcessor.class);

    new EventDockKafkaConsumerManager(
        List.of(inboxBinding, directBinding),
        factory,
        new KafkaEventRecordMapper(KafkaTopicResolver.eventType()),
        repository,
        Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
        mock(InboxProcessor.class),
        directProcessor,
        false);

    listener(inboxProperties).onMessage(record("member.created"));
    listener(directProperties).onMessage(record("order.created"));

    verify(repository)
        .receive(
            eq("member-created"),
            argThat(event -> event.type().equals("member.created")),
            eq(Instant.EPOCH));
    verify(directProcessor)
        .process(
            eq("notifications"), argThat(event -> event.type().equals("order.created")));
    org.junit.jupiter.api.Assertions.assertEquals("member-group", inboxProperties.getGroupId());
    org.junit.jupiter.api.Assertions.assertEquals("notification-group", directProperties.getGroupId());
  }

  private EventDockProperties.ConsumerBinding binding(
      String id, ConsumerMode mode, String groupId, String eventType, String routedConsumerId) {
    var binding = new EventDockProperties.ConsumerBinding();
    binding.setId(id);
    binding.setMode(mode);
    binding.setTopics(List.of(eventType));
    binding.getKafka().setGroupId(groupId);
    if (routedConsumerId != null) {
      var route = new EventDockProperties.Route();
      route.setEventType(eventType);
      route.setConsumerId(routedConsumerId);
      binding.setRoutes(List.of(route));
    }
    return binding;
  }

  private MessageListener<String, byte[]> listener(ContainerProperties properties) {
    return (MessageListener<String, byte[]>) properties.getMessageListener();
  }

  private ConsumerRecord<String, byte[]> record(String type) {
    var mapper = new KafkaEventRecordMapper(KafkaTopicResolver.eventType());
    var produced = mapper.toProducerRecord(event(type));
    var consumed = new ConsumerRecord<String, byte[]>(type, 0, 0, produced.key(), produced.value());
    produced.headers().forEach(header -> consumed.headers().add(header));
    return consumed;
  }

  private SerializedEvent event(String type) {
    return new SerializedEvent(
        new EventId(type + "-1"),
        type,
        1,
        new AggregateRef("order", "1", 1),
        Instant.EPOCH,
        "application/json",
        "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8),
        Map.of());
  }
}
