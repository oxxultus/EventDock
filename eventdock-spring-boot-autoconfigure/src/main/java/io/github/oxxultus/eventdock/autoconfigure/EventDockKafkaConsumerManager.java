package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.core.ConsumerMode;
import io.github.oxxultus.eventdock.core.DirectEventProcessor;
import io.github.oxxultus.eventdock.inbox.InboxProcessor;
import io.github.oxxultus.eventdock.inbox.InboxRepository;
import io.github.oxxultus.eventdock.transport.kafka.KafkaEventRecordMapper;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

public final class EventDockKafkaConsumerManager implements SmartLifecycle {
  private final List<ConcurrentMessageListenerContainer<String, byte[]>> containers;
  private final boolean autoStartup;
  private volatile boolean running;

  public EventDockKafkaConsumerManager(
      List<EventDockProperties.ConsumerBinding> bindings,
      ConcurrentKafkaListenerContainerFactory<String, byte[]> factory,
      KafkaEventRecordMapper mapper,
      InboxRepository inboxRepository,
      Clock clock,
      InboxProcessor inboxProcessor,
      DirectEventProcessor directProcessor,
      boolean autoStartup) {
    Objects.requireNonNull(bindings);
    this.autoStartup = autoStartup;
    containers = new ArrayList<>(bindings.size());
    for (EventDockProperties.ConsumerBinding binding : bindings) {
      requireProcessor(binding, inboxProcessor, directProcessor);
      var container = factory.createContainer(binding.getTopics().toArray(String[]::new));
      container.setBeanName("eventDockKafkaConsumer-" + binding.getId());
      container.getContainerProperties().setGroupId(binding.groupId());
      container
          .getContainerProperties()
          .setMessageListener(
              (MessageListener<String, byte[]>)
                  record ->
                      receive(
                          binding,
                          record,
                          mapper,
                          inboxRepository,
                          clock,
                          directProcessor));
      containers.add(container);
    }
  }

  private void receive(
      EventDockProperties.ConsumerBinding binding,
      ConsumerRecord<String, byte[]> record,
      KafkaEventRecordMapper mapper,
      InboxRepository inboxRepository,
      Clock clock,
      DirectEventProcessor directProcessor) {
    var event = mapper.fromConsumerRecord(record);
    String consumerId = binding.consumerId(event.type());
    if (binding.getMode() == ConsumerMode.INBOX) {
      inboxRepository.receive(consumerId, event, clock.instant());
    } else {
      directProcessor.process(consumerId, event);
    }
  }

  private void requireProcessor(
      EventDockProperties.ConsumerBinding binding,
      InboxProcessor inboxProcessor,
      DirectEventProcessor directProcessor) {
    if (binding.getMode() == ConsumerMode.INBOX && inboxProcessor == null) {
      throw new IllegalStateException("InboxHandlerRegistry is required for INBOX binding " + binding.getId());
    }
    if (binding.getMode() == ConsumerMode.DIRECT && directProcessor == null) {
      throw new IllegalStateException("EventHandlerRegistry is required for DIRECT binding " + binding.getId());
    }
  }

  @Override
  public void start() {
    containers.forEach(ConcurrentMessageListenerContainer::start);
    running = true;
  }

  @Override
  public void stop() {
    containers.forEach(ConcurrentMessageListenerContainer::stop);
    running = false;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public boolean isAutoStartup() {
    return autoStartup;
  }

  List<ConcurrentMessageListenerContainer<String, byte[]>> containers() {
    return List.copyOf(containers);
  }
}
