package io.github.oxxultus.eventdock.autoconfigure;

import static org.mockito.Mockito.mock;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.oxxultus.eventdock.core.UnitOfWork;
import io.github.oxxultus.eventdock.core.DirectEventWriter;
import io.github.oxxultus.eventdock.core.EventPublisher;
import io.github.oxxultus.eventdock.core.EventWriter;
import io.github.oxxultus.eventdock.inbox.InboxHandlerRegistry;
import io.github.oxxultus.eventdock.outbox.OutboxWriter;
import io.github.oxxultus.eventdock.storage.postgresql.PostgresqlStorage;
import io.github.oxxultus.eventdock.transport.kafka.KafkaDirectReceiver;
import io.github.oxxultus.eventdock.transport.kafka.KafkaInboxReceiver;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.ObjectMapper;

class EventDockAutoConfigurationTest {
  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withUserConfiguration(Dependencies.class)
          .withPropertyValues("eventdock.initialize-schema=false")
          .withConfiguration(
              org.springframework.boot.autoconfigure.AutoConfigurations.of(
                  EventDockAutoConfiguration.class));

  @Test
  void createsStorageWriterPublisherAndSpringUnitOfWork() {
    runner.run(
        context -> {
          context.getBean(PostgresqlStorage.class);
          context.getBean(OutboxWriter.class);
          context.getBean(UnitOfWork.class);
          context.getBean(EventPublisher.class);
        });
  }

  @Test
  void supportsAllFourProducerConsumerModeCombinations() {
    for (String producer : new String[] {"outbox", "direct"}) {
      for (String consumer : new String[] {"inbox", "direct"}) {
        runner
            .withUserConfiguration(Handlers.class)
            .withPropertyValues(
                "eventdock.producer.mode=" + producer,
                "eventdock.consumer.mode=" + consumer,
                "eventdock.inbox.enabled=true",
                "eventdock.inbox.consumer-id=billing",
                "eventdock.inbox.topics=order.created")
            .run(
                context -> {
                  EventWriter writer = context.getBean(EventWriter.class);
                  if (producer.equals("outbox")) {
                    assertInstanceOf(OutboxWriter.class, writer);
                  } else {
                    assertInstanceOf(DirectEventWriter.class, writer);
                  }
                  if (consumer.equals("inbox")) {
                    context.getBean(KafkaInboxReceiver.class);
                    assertTrue(context.getBeansOfType(KafkaDirectReceiver.class).isEmpty());
                  } else {
                    context.getBean(KafkaDirectReceiver.class);
                    assertTrue(context.getBeansOfType(KafkaInboxReceiver.class).isEmpty());
                  }
                });
      }
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class Dependencies {
    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Bean
    PlatformTransactionManager transactionManager() {
      return mock(PlatformTransactionManager.class);
    }

    @Bean
    ObjectMapper objectMapper() {
      return new ObjectMapper();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class Handlers {
    @Bean
    InboxHandlerRegistry inboxHandlerRegistry() {
      return (consumerId, eventType) -> event -> {};
    }
  }
}
