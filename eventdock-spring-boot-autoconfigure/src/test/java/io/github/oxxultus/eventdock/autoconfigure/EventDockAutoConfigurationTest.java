package io.github.oxxultus.eventdock.autoconfigure;

import static org.mockito.Mockito.mock;

import io.github.oxxultus.eventdock.core.UnitOfWork;
import io.github.oxxultus.eventdock.core.EventPublisher;
import io.github.oxxultus.eventdock.outbox.OutboxWriter;
import io.github.oxxultus.eventdock.storage.postgresql.PostgresqlStorage;
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
}
