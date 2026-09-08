package io.github.oxxultus.eventdock.transport.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers(disabledWithoutDocker = true)
class KafkaTransportIntegrationTest {
  @Container
  private static final KafkaContainer KAFKA =
      new KafkaContainer("apache/kafka-native:3.8.0");

  @Test
  void publishesAndConsumesCompleteEventEnvelope() throws Exception {
    var mapper = new KafkaEventRecordMapper(KafkaTopicResolver.eventType());
    var event = event();
    try (var producer = new KafkaProducer<String, byte[]>(producerProperties())) {
      producer.send(mapper.toProducerRecord(event)).get();
    }

    try (var consumer = new KafkaConsumer<String, byte[]>(consumerProperties())) {
      consumer.subscribe(List.of(event.type()));
      SerializedEvent consumed = null;
      var deadline = Instant.now().plusSeconds(15);
      while (consumed == null && Instant.now().isBefore(deadline)) {
        var records = consumer.poll(Duration.ofMillis(250));
        if (!records.isEmpty()) {
          consumed = mapper.fromConsumerRecord(records.iterator().next());
        }
      }
      assertNotNull(consumed, "Kafka record was not consumed before timeout");
      assertEquals(event.id(), consumed.id());
      assertEquals(event.aggregate(), consumed.aggregate());
      assertEquals(event.metadata(), consumed.metadata());
    }
  }

  private Properties producerProperties() {
    var properties = new Properties();
    properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", ByteArraySerializer.class.getName());
    properties.put("acks", "all");
    return properties;
  }

  private Properties consumerProperties() {
    var properties = new Properties();
    properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
    properties.put("group.id", "eventdock-integration");
    properties.put("auto.offset.reset", "earliest");
    properties.put("key.deserializer", StringDeserializer.class.getName());
    properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
    return properties;
  }

  private SerializedEvent event() {
    return new SerializedEvent(
        new EventId("kafka-e2e"),
        "order.created",
        1,
        new AggregateRef("order", "42", 2),
        Instant.parse("2026-09-08T12:00:00Z"),
        "application/json",
        "{\"orderId\":42}".getBytes(StandardCharsets.UTF_8),
        Map.of("trace-id", "trace-1"));
  }
}
