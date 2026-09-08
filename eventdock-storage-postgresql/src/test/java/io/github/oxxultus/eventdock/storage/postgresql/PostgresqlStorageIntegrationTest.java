package io.github.oxxultus.eventdock.storage.postgresql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import io.github.oxxultus.eventdock.inbox.AggregateKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers(disabledWithoutDocker = true)
class PostgresqlStorageIntegrationTest {
  @Container
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:17-alpine");

  private static PGSimpleDataSource dataSource;
  private PostgresqlStorage storage;

  @BeforeAll
  static void createSchema() throws SQLException, IOException {
    dataSource = new PGSimpleDataSource();
    dataSource.setURL(POSTGRES.getJdbcUrl());
    dataSource.setUser(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());
    try (var input =
            PostgresqlStorageIntegrationTest.class
                .getClassLoader()
                .getResourceAsStream(PostgresqlStorage.MIGRATION_RESOURCE);
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      if (input == null) {
        throw new IllegalStateException("migration resource not found");
      }
      statement.execute(new String(input.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @BeforeEach
  void resetDatabase() throws SQLException {
    storage = new PostgresqlStorage(dataSource);
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "TRUNCATE eventdock.outbox_events, eventdock.inbox_events, "
              + "eventdock.inbox_aggregate_versions");
    }
  }

  @Test
  void outboxPreservesEventAndCompletesPublication() throws SQLException {
    var event = event("outbox-1", 3);
    var claimAt = Instant.now().plusSeconds(5);
    storage.outboxRepository().append(event);
    var claimed =
        storage
            .outboxRepository()
            .claim(10, claimAt, Duration.ofSeconds(30));

    assertEquals(1, claimed.size());
    assertEquals(event.id(), claimed.getFirst().event().id());
    assertEquals(event.metadata(), claimed.getFirst().event().metadata());
    assertArrayEquals(event.payload(), claimed.getFirst().event().payload());
    storage
        .outboxRepository()
        .markPublished(event.id(), claimAt.plusSeconds(1));
    assertEquals("PUBLISHED", value("SELECT status FROM eventdock.outbox_events", String.class));
  }

  @Test
  void outboxRecoversExpiredLockAndHonorsRetryAvailability() throws SQLException {
    var event = event("outbox-retry", 1);
    var repository = storage.outboxRepository();
    var firstClaim = Instant.now().plusSeconds(5);
    repository.append(event);
    assertEquals(1, repository.claim(1, firstClaim, Duration.ofSeconds(30)).size());
    assertEquals(0, repository.claim(1, firstClaim.plusSeconds(10), Duration.ofSeconds(30)).size());
    assertEquals(1, repository.claim(1, firstClaim.plusSeconds(31), Duration.ofSeconds(30)).size());
    repository.recordFailure(event.id(), "temporary", firstClaim.plusSeconds(60), false);
    assertEquals(0, repository.claim(1, firstClaim.plusSeconds(59), Duration.ofSeconds(30)).size());
    assertEquals(1, repository.claim(1, firstClaim.plusSeconds(60), Duration.ofSeconds(30)).size());
    repository.recordFailure(event.id(), "exhausted", firstClaim.plusSeconds(90), true);

    assertEquals("FAILED", value("SELECT status FROM eventdock.outbox_events", String.class));
    assertEquals(2, value("SELECT retry_count FROM eventdock.outbox_events", Integer.class));
  }

  @Test
  void inboxDeduplicatesPerConsumerAndPersistsSkippedState() throws SQLException {
    var event = event("inbox-1", 2);
    var repository = storage.inboxRepository();
    var receivedAt = Instant.parse("2026-09-08T08:00:00Z");

    assertTrue(repository.receive("billing", event, receivedAt));
    assertFalse(repository.receive("billing", event, receivedAt.plusSeconds(1)));
    assertTrue(repository.receive("notification", event, receivedAt));
    var claimed = repository.claim("billing", 10, receivedAt.plusSeconds(1), Duration.ofSeconds(30));
    assertEquals(1, claimed.size());
    repository.markSkipped("billing", event.id(), "OLDER_THAN_LAST_APPLIED", receivedAt.plusSeconds(2));

    assertEquals(
        "SKIPPED",
        value(
            "SELECT status FROM eventdock.inbox_events WHERE consumer_id = 'billing'",
            String.class));
  }

  @Test
  void aggregateProgressRequiresUnitOfWorkAndPersistsAdvance() {
    var key = new AggregateKey("billing", "order", "aggregate-42");
    var repository = storage.inboxRepository();

    assertThrows(
        PostgresqlStorageException.class,
        () -> repository.getOrCreateAndLock(key, Instant.EPOCH));
    storage
        .unitOfWork()
        .execute(
            () -> {
              assertEquals(0, repository.getOrCreateAndLock(key, Instant.EPOCH));
              repository.advance(key, 5, Instant.EPOCH.plusSeconds(1));
            });
    long stored =
        storage
            .unitOfWork()
            .execute(() -> repository.getOrCreateAndLock(key, Instant.EPOCH.plusSeconds(2)));

    assertEquals(5, stored);
  }

  @Test
  void concurrentWorkersClaimAnEventOnlyOnce() throws Exception {
    storage.outboxRepository().append(event("concurrent-1", 1));
    var start = new CountDownLatch(1);
    var claimAt = Instant.now().plusSeconds(5);

    try (var workers = Executors.newFixedThreadPool(2)) {
      var first =
          workers.submit(
              () -> {
                start.await();
                return storage.outboxRepository().claim(1, claimAt, Duration.ofMinutes(1)).size();
              });
      var second =
          workers.submit(
              () -> {
                start.await();
                return storage.outboxRepository().claim(1, claimAt, Duration.ofMinutes(1)).size();
              });
      start.countDown();

      assertEquals(1, first.get() + second.get());
    }
  }

  @Test
  void cleanupDeletesOnlyCompletedRecordsBeforeRetentionBoundary() {
    var old = Instant.parse("2026-01-01T00:00:00Z");
    var future = Instant.parse("2099-01-01T00:00:00Z");
    var outbox = event("cleanup-outbox", 1);
    var inbox = event("cleanup-inbox", 1);
    storage.outboxRepository().append(outbox);
    storage.outboxRepository().claim(1, future, Duration.ofMinutes(1));
    storage.outboxRepository().markPublished(outbox.id(), old);
    storage.inboxRepository().receive("consumer", inbox, old);
    storage.inboxRepository().claim("consumer", 1, old.plusSeconds(1), Duration.ofMinutes(1));
    storage.inboxRepository().markProcessed("consumer", inbox.id(), old.plusSeconds(2));
    storage.outboxRepository().append(event("pending-outbox", 1));

    assertEquals(1, storage.outboxRepository().deletePublishedBefore(future, 100));
    assertEquals(1, storage.inboxRepository().deleteCompletedBefore(future, 100));
  }

  private static SerializedEvent event(String id, long version) {
    return new SerializedEvent(
        new EventId(id),
        "order.created",
        1,
        new AggregateRef("order", "aggregate-42", version),
        Instant.parse("2026-09-08T07:59:00Z"),
        "application/json",
        "{\"orderId\":42}".getBytes(StandardCharsets.UTF_8),
        Map.of("correlation:id", "추적-1"));
  }

  private <T> T value(String sql, Class<T> type) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      result.next();
      return result.getObject(1, type);
    }
  }
}
