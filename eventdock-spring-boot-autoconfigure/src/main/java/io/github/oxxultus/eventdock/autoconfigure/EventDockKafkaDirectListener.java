package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.transport.kafka.KafkaDirectReceiver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

public final class EventDockKafkaDirectListener {
  private final KafkaDirectReceiver receiver;

  public EventDockKafkaDirectListener(KafkaDirectReceiver receiver) {
    this.receiver = receiver;
  }

  @KafkaListener(
      topics = "#{'${eventdock.inbox.topics}'.split(',')}",
      groupId = "${eventdock.inbox.consumer-id}",
      containerFactory = "eventDockKafkaListenerContainerFactory")
  public void receive(ConsumerRecord<String, byte[]> record) {
    receiver.receive(record);
  }
}
