package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.core.EventCodec;
import io.github.oxxultus.eventdock.core.EventPublisher;
import io.github.oxxultus.eventdock.core.ExponentialBackoffRetryPolicy;
import io.github.oxxultus.eventdock.core.RetryPolicy;
import io.github.oxxultus.eventdock.core.UnitOfWork;
import io.github.oxxultus.eventdock.inbox.AggregateVersionRepository;
import io.github.oxxultus.eventdock.inbox.InboxExhaustionHandler;
import io.github.oxxultus.eventdock.inbox.InboxHandlerRegistry;
import io.github.oxxultus.eventdock.inbox.InboxProcessor;
import io.github.oxxultus.eventdock.inbox.InboxRepository;
import io.github.oxxultus.eventdock.outbox.OutboxExhaustionHandler;
import io.github.oxxultus.eventdock.outbox.OutboxProcessor;
import io.github.oxxultus.eventdock.outbox.OutboxRepository;
import io.github.oxxultus.eventdock.outbox.OutboxWriter;
import io.github.oxxultus.eventdock.storage.postgresql.PostgresqlStorage;
import io.github.oxxultus.eventdock.transport.kafka.KafkaEventPublisher;
import io.github.oxxultus.eventdock.transport.kafka.KafkaEventRecordMapper;
import io.github.oxxultus.eventdock.transport.kafka.KafkaInboxReceiver;
import io.github.oxxultus.eventdock.transport.kafka.KafkaSender;
import io.github.oxxultus.eventdock.transport.kafka.KafkaTopicResolver;
import java.time.Clock;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties({EventDockProperties.class, KafkaProperties.class})
public class EventDockAutoConfiguration {
  @Bean
  @ConditionalOnMissingBean
  Clock eventDockClock() {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean
  RetryPolicy eventDockRetryPolicy(EventDockProperties properties) {
    var retry = properties.getRetry();
    return new ExponentialBackoffRetryPolicy(
        retry.getMaxAttempts(),
        retry.getInitialDelay(),
        retry.getMaxDelay(),
        retry.getLockTimeout());
  }

  @Bean
  @ConditionalOnMissingBean
  PostgresqlStorage eventDockPostgresqlStorage(DataSource dataSource) {
    return new PostgresqlStorage(new TransactionAwareDataSourceProxy(dataSource));
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxRepository eventDockOutboxRepository(PostgresqlStorage storage) {
    return storage.outboxRepository();
  }

  @Bean
  @ConditionalOnMissingBean
  InboxRepository eventDockInboxRepository(PostgresqlStorage storage) {
    return storage.inboxRepository();
  }

  @Bean
  @ConditionalOnMissingBean
  AggregateVersionRepository eventDockAggregateVersionRepository(PostgresqlStorage storage) {
    return storage.inboxRepository();
  }

  @Bean
  @ConditionalOnMissingBean
  UnitOfWork eventDockUnitOfWork(PlatformTransactionManager transactionManager) {
    return new SpringTransactionUnitOfWork(new TransactionTemplate(transactionManager));
  }

  @Bean
  @ConditionalOnProperty(
      name = "eventdock.initialize-schema",
      havingValue = "true",
      matchIfMissing = true)
  InitializingBean eventDockSchemaInitializer(DataSource dataSource) {
    return () ->
        new ResourceDatabasePopulator(
                new ClassPathResource(PostgresqlStorage.MIGRATION_RESOURCE))
            .execute(dataSource);
  }

  @Bean
  @ConditionalOnMissingBean
  EventCodec eventDockEventCodec(ObjectMapper objectMapper) {
    return new JacksonEventCodec(objectMapper);
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxWriter eventDockOutboxWriter(OutboxRepository repository, EventCodec codec) {
    return new OutboxWriter(repository, codec);
  }

  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(KafkaTemplate.class)
  static class KafkaConfiguration {
    @Bean
    @ConditionalOnMissingBean
    KafkaTopicResolver eventDockKafkaTopicResolver() {
      return KafkaTopicResolver.eventType();
    }

    @Bean
    @ConditionalOnMissingBean
    KafkaEventRecordMapper eventDockKafkaEventRecordMapper(KafkaTopicResolver resolver) {
      return new KafkaEventRecordMapper(resolver);
    }

    @Bean
    @ConditionalOnMissingBean
    KafkaSender eventDockKafkaSender(KafkaProperties kafka, EventDockProperties eventDock) {
      var producerFactory =
          new DefaultKafkaProducerFactory<String, byte[]>(
              kafka.buildProducerProperties(), new StringSerializer(), new ByteArraySerializer());
      return new SpringKafkaSender(producerFactory, eventDock.getPublishTimeout());
    }

    @Bean
    @ConditionalOnMissingBean(EventPublisher.class)
    EventPublisher eventDockEventPublisher(KafkaEventRecordMapper mapper, KafkaSender sender) {
      return new KafkaEventPublisher(mapper, sender);
    }
  }

  @Bean
  @ConditionalOnBean(EventPublisher.class)
  @ConditionalOnMissingBean
  OutboxProcessor eventDockOutboxProcessor(
      OutboxRepository repository,
      EventPublisher publisher,
      RetryPolicy retry,
      Clock clock,
      UnitOfWork unitOfWork) {
    return new OutboxProcessor(
        repository, publisher, retry, clock, unitOfWork, OutboxExhaustionHandler.noOp());
  }

  @Bean
  @ConditionalOnBean(OutboxProcessor.class)
  @ConditionalOnProperty(
      name = "eventdock.outbox.enabled",
      havingValue = "true",
      matchIfMissing = true)
  EventDockOutboxScheduler eventDockOutboxScheduler(
      OutboxProcessor processor, EventDockProperties properties) {
    return new EventDockOutboxScheduler(processor, properties);
  }

  @Configuration(proxyBeanMethods = false)
  @EnableKafka
  @ConditionalOnBean(InboxHandlerRegistry.class)
  @ConditionalOnProperty(name = {"eventdock.inbox.consumer-id", "eventdock.inbox.topics"})
  static class InboxConfiguration {
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, byte[]>
        eventDockKafkaListenerContainerFactory(KafkaProperties properties) {
      var consumerFactory =
          new DefaultKafkaConsumerFactory<>(
              properties.buildConsumerProperties(),
              new StringDeserializer(),
              new ByteArrayDeserializer());
      var factory = new ConcurrentKafkaListenerContainerFactory<String, byte[]>();
      factory.setConsumerFactory(consumerFactory);
      return factory;
    }

    @Bean
    @ConditionalOnMissingBean
    InboxProcessor eventDockInboxProcessor(
        InboxRepository repository,
        InboxHandlerRegistry handlers,
        AggregateVersionRepository versions,
        RetryPolicy retry,
        Clock clock,
        UnitOfWork unitOfWork) {
      return new InboxProcessor(
          repository,
          handlers,
          versions,
          retry,
          clock,
          unitOfWork,
          InboxExhaustionHandler.noOp());
    }

    @Bean
    KafkaInboxReceiver eventDockKafkaInboxReceiver(
        EventDockProperties properties,
        KafkaEventRecordMapper mapper,
        InboxRepository repository,
        Clock clock) {
      return new KafkaInboxReceiver(
          properties.getInbox().getConsumerId(), mapper, repository, clock);
    }

    @Bean
    EventDockKafkaInboxListener eventDockKafkaInboxListener(KafkaInboxReceiver receiver) {
      return new EventDockKafkaInboxListener(receiver);
    }

    @Bean
    @ConditionalOnProperty(
        name = "eventdock.inbox.enabled",
        havingValue = "true",
        matchIfMissing = true)
    EventDockInboxScheduler eventDockInboxScheduler(
        InboxProcessor processor, EventDockProperties properties) {
      return new EventDockInboxScheduler(processor, properties);
    }
  }
}
