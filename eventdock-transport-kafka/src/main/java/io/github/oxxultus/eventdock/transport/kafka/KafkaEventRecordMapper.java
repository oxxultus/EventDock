package io.github.oxxultus.eventdock.transport.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

public final class KafkaEventRecordMapper {
  private final KafkaTopicResolver topicResolver;

  public KafkaEventRecordMapper(KafkaTopicResolver topicResolver) {
    this.topicResolver = Objects.requireNonNull(topicResolver);
  }

  public ProducerRecord<String, byte[]> toProducerRecord(SerializedEvent event) {
    var record =
        new ProducerRecord<String, byte[]>(
            requireTopic(topicResolver.resolve(event)),
            event.aggregate().type() + ":" + event.aggregate().id(),
            event.payload());
    add(record.headers(), KafkaEventHeaders.EVENT_ID, event.id().value());
    add(record.headers(), KafkaEventHeaders.EVENT_TYPE, event.type());
    add(record.headers(), KafkaEventHeaders.SCHEMA_VERSION, event.schemaVersion());
    add(record.headers(), KafkaEventHeaders.AGGREGATE_TYPE, event.aggregate().type());
    add(record.headers(), KafkaEventHeaders.AGGREGATE_ID, event.aggregate().id());
    add(record.headers(), KafkaEventHeaders.AGGREGATE_VERSION, event.aggregate().version());
    add(record.headers(), KafkaEventHeaders.OCCURRED_AT, event.occurredAt());
    add(record.headers(), KafkaEventHeaders.CONTENT_TYPE, event.contentType());
    event.metadata()
        .forEach(
            (key, value) ->
                add(record.headers(), KafkaEventHeaders.METADATA_PREFIX + key, value));
    return record;
  }

  public SerializedEvent fromConsumerRecord(ConsumerRecord<String, byte[]> record) {
    Headers headers = record.headers();
    return new SerializedEvent(
        new EventId(required(headers, KafkaEventHeaders.EVENT_ID)),
        required(headers, KafkaEventHeaders.EVENT_TYPE),
        integer(headers, KafkaEventHeaders.SCHEMA_VERSION),
        new AggregateRef(
            required(headers, KafkaEventHeaders.AGGREGATE_TYPE),
            required(headers, KafkaEventHeaders.AGGREGATE_ID),
            number(headers, KafkaEventHeaders.AGGREGATE_VERSION)),
        Instant.parse(required(headers, KafkaEventHeaders.OCCURRED_AT)),
        required(headers, KafkaEventHeaders.CONTENT_TYPE),
        record.value(),
        metadata(headers));
  }

  private Map<String, String> metadata(Headers headers) {
    Map<String, String> metadata = new HashMap<>();
    headers.forEach(
        header -> {
          if (header.key().startsWith(KafkaEventHeaders.METADATA_PREFIX)) {
            metadata.put(
                header.key().substring(KafkaEventHeaders.METADATA_PREFIX.length()),
                new String(header.value(), UTF_8));
          }
        });
    return metadata;
  }

  private String required(Headers headers, String key) {
    var header = headers.lastHeader(key);
    if (header == null || header.value() == null) {
      throw new IllegalArgumentException("missing required Kafka header: " + key);
    }
    return new String(header.value(), UTF_8);
  }

  private int integer(Headers headers, String key) {
    return Integer.parseInt(required(headers, key));
  }

  private long number(Headers headers, String key) {
    return Long.parseLong(required(headers, key));
  }

  private void add(Headers headers, String key, Object value) {
    headers.add(new RecordHeader(key, value.toString().getBytes(UTF_8)));
  }

  private String requireTopic(String topic) {
    if (topic == null || !topic.matches("[a-zA-Z0-9._-]+")) {
      throw new IllegalArgumentException("invalid Kafka topic: " + topic);
    }
    return topic;
  }
}
