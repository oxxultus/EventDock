package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.transport.kafka.KafkaSender;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public final class SpringKafkaSender implements KafkaSender, DisposableBean {
  private final DefaultKafkaProducerFactory<String, byte[]> producerFactory;
  private final KafkaTemplate<String, byte[]> template;
  private final Duration timeout;

  public SpringKafkaSender(
      DefaultKafkaProducerFactory<String, byte[]> producerFactory, Duration timeout) {
    this.producerFactory = producerFactory;
    this.template = new KafkaTemplate<>(producerFactory);
    this.timeout = timeout;
  }

  @Override
  public void send(ProducerRecord<String, byte[]> record) {
    try {
      template.send(record).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Kafka publish interrupted", exception);
    } catch (Exception exception) {
      throw new IllegalStateException("Kafka publish failed", exception);
    }
  }

  @Override
  public void destroy() {
    producerFactory.destroy();
  }
}
