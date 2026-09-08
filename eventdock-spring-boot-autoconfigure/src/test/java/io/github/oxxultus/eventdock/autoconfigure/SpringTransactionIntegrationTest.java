package io.github.oxxultus.eventdock.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventEnvelope;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.outbox.OutboxWriter;
import io.github.oxxultus.eventdock.storage.postgresql.PostgresqlStorage;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

@Testcontainers(disabledWithoutDocker = true)
class SpringTransactionIntegrationTest {
  @Container
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:17-alpine");

  private static PGSimpleDataSource dataSource;

  @BeforeAll
  static void initialize() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setURL(POSTGRES.getJdbcUrl());
    dataSource.setUser(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());
    try (var input =
            SpringTransactionIntegrationTest.class
                .getClassLoader()
                .getResourceAsStream(PostgresqlStorage.MIGRATION_RESOURCE);
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(new String(input.readAllBytes(), StandardCharsets.UTF_8));
      statement.execute("CREATE TABLE domain_marker (id bigint PRIMARY KEY)");
    }
  }

  @Test
  void domainAndOutboxRollBackTogether() {
    var jdbc = new JdbcTemplate(dataSource);
    var storage = new PostgresqlStorage(new TransactionAwareDataSourceProxy(dataSource));
    var writer = new OutboxWriter(storage.outboxRepository(), new JacksonEventCodec(new ObjectMapper()));
    var transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));

    assertThrows(
        IllegalStateException.class,
        () ->
            transaction.executeWithoutResult(
                status -> {
                  jdbc.update("INSERT INTO domain_marker (id) VALUES (1)");
                  writer.append(event());
                  throw new IllegalStateException("force rollback");
                }));

    assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM domain_marker", Long.class));
    assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM eventdock.outbox_events", Long.class));
  }

  private EventEnvelope<Map<String, Long>> event() {
    return new EventEnvelope<>(
        new EventId("rollback-event"),
        "order.created",
        1,
        new AggregateRef("order", "1", 1),
        Instant.now(),
        Map.of("orderId", 1L),
        Map.of());
  }
}
