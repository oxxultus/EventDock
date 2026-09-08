package io.github.oxxultus.eventdock.transport.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;

@FunctionalInterface
public interface KafkaSender {
  void send(ProducerRecord<String, byte[]> record);
}
