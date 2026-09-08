package io.github.oxxultus.eventdock.storage.postgresql;

import static io.github.oxxultus.eventdock.storage.postgresql.JdbcValues.instant;
import static io.github.oxxultus.eventdock.storage.postgresql.JdbcValues.requireOne;
import static io.github.oxxultus.eventdock.storage.postgresql.JdbcValues.timestamp;

import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import io.github.oxxultus.eventdock.inbox.AggregateKey;
import io.github.oxxultus.eventdock.inbox.AggregateVersionRepository;
import io.github.oxxultus.eventdock.inbox.InboxEntry;
import io.github.oxxultus.eventdock.inbox.InboxRepository;
import io.github.oxxultus.eventdock.inbox.InboxCleanupRepository;
import io.github.oxxultus.eventdock.inbox.InboxStatus;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class PostgresqlInboxRepository
    implements InboxRepository, AggregateVersionRepository, InboxCleanupRepository {
  private static final String RECEIVE_SQL =
      """
      INSERT INTO eventdock.inbox_events (
          consumer_id, event_id, event_type, schema_version,
          aggregate_type, aggregate_id, aggregate_version,
          occurred_at, content_type, payload, metadata,
          status, retry_count, available_at, received_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RECEIVED', 0, ?, ?)
      ON CONFLICT (consumer_id, event_id) DO NOTHING
      """;

  private static final String CLAIM_SQL =
      """
      WITH candidates AS (
          SELECT consumer_id, event_id
            FROM eventdock.inbox_events
           WHERE consumer_id = ?
             AND ((status = 'RECEIVED' AND available_at <= ?)
               OR (status = 'PROCESSING' AND locked_at < ?))
           ORDER BY available_at, received_at, event_id
           LIMIT ?
           FOR UPDATE SKIP LOCKED
      )
      UPDATE eventdock.inbox_events AS inbox
         SET status = 'PROCESSING', locked_at = ?
        FROM candidates
       WHERE inbox.consumer_id = candidates.consumer_id
         AND inbox.event_id = candidates.event_id
      RETURNING inbox.*
      """;

  private final JdbcConnectionContext connections;
  private final MetadataCodec metadata;

  PostgresqlInboxRepository(JdbcConnectionContext connections, MetadataCodec metadata) {
    this.connections = Objects.requireNonNull(connections);
    this.metadata = Objects.requireNonNull(metadata);
  }

  @Override
  public boolean receive(String consumerId, SerializedEvent event, Instant receivedAt) {
    requireConsumer(consumerId);
    Objects.requireNonNull(event, "event must not be null");
    Objects.requireNonNull(receivedAt, "receivedAt must not be null");
    return connections.withConnection(
        connection -> {
          try (PreparedStatement statement = connection.prepareStatement(RECEIVE_SQL)) {
            statement.setString(1, consumerId);
            bindEvent(statement, event, 2);
            statement.setString(11, metadata.encode(event.metadata()));
            statement.setObject(12, timestamp(receivedAt));
            statement.setObject(13, timestamp(receivedAt));
            return statement.executeUpdate() == 1;
          }
        });
  }

  @Override
  public List<InboxEntry> claim(
      String consumerId, int limit, Instant now, Duration lockTimeout) {
    requireConsumer(consumerId);
    requireClaimArguments(limit, now, lockTimeout);
    return connections.inTransaction(
        connection -> {
          try (PreparedStatement statement = connection.prepareStatement(CLAIM_SQL)) {
            statement.setString(1, consumerId);
            statement.setObject(2, timestamp(now));
            statement.setObject(3, timestamp(now.minus(lockTimeout)));
            statement.setInt(4, limit);
            statement.setObject(5, timestamp(now));
            try (ResultSet result = statement.executeQuery()) {
              List<InboxEntry> entries = new ArrayList<>();
              while (result.next()) {
                entries.add(toEntry(result));
              }
              return List.copyOf(entries);
            }
          }
        });
  }

  @Override
  public void markProcessed(String consumerId, EventId eventId, Instant processedAt) {
    updateCompleted(consumerId, eventId, "PROCESSED", null, processedAt);
  }

  @Override
  public void markSkipped(
      String consumerId, EventId eventId, String reason, Instant processedAt) {
    Objects.requireNonNull(reason, "reason must not be null");
    updateCompleted(consumerId, eventId, "SKIPPED", truncate(reason), processedAt);
  }

  @Override
  public void recordFailure(
      String consumerId,
      EventId eventId,
      String reason,
      Instant nextAttemptAt,
      boolean exhausted) {
    requireIdentity(consumerId, eventId);
    Objects.requireNonNull(reason, "reason must not be null");
    Objects.requireNonNull(nextAttemptAt, "nextAttemptAt must not be null");
    connections.withConnection(
        connection -> {
          try (PreparedStatement statement =
              connection.prepareStatement(
                  """
                  UPDATE eventdock.inbox_events
                     SET status = ?, retry_count = retry_count + 1,
                         last_error = ?, available_at = ?, locked_at = NULL
                   WHERE consumer_id = ? AND event_id = ? AND status = 'PROCESSING'
                  """)) {
            statement.setString(1, exhausted ? "FAILED" : "RECEIVED");
            statement.setString(2, truncate(reason));
            statement.setObject(3, timestamp(nextAttemptAt));
            statement.setString(4, consumerId);
            statement.setString(5, eventId.value());
            requireOne(statement.executeUpdate(), "record inbox failure", identity(consumerId, eventId));
          }
          return null;
        });
  }

  @Override
  public long getOrCreateAndLock(AggregateKey key, Instant now) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(now, "now must not be null");
    return connections.requireTransaction(
        connection -> {
          try (PreparedStatement insert =
              connection.prepareStatement(
                  """
                  INSERT INTO eventdock.inbox_aggregate_versions (
                      consumer_id, aggregate_type, aggregate_id, last_processed_version, updated_at
                  ) VALUES (?, ?, ?, 0, ?)
                  ON CONFLICT (consumer_id, aggregate_type, aggregate_id) DO NOTHING
                  """)) {
            insert.setString(1, key.consumerId());
            insert.setString(2, key.aggregateType());
            insert.setString(3, key.aggregateId());
            insert.setObject(4, timestamp(now));
            insert.executeUpdate();
          }
          try (PreparedStatement select =
              connection.prepareStatement(
                  """
                  SELECT last_processed_version
                    FROM eventdock.inbox_aggregate_versions
                   WHERE consumer_id = ? AND aggregate_type = ? AND aggregate_id = ?
                   FOR UPDATE
                  """)) {
            select.setString(1, key.consumerId());
            select.setString(2, key.aggregateType());
            select.setString(3, key.aggregateId());
            try (ResultSet result = select.executeQuery()) {
              if (!result.next()) {
                throw new PostgresqlStorageException("aggregate progress not found: " + key);
              }
              return result.getLong(1);
            }
          }
        });
  }

  @Override
  public void advance(AggregateKey key, long version, Instant now) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(now, "now must not be null");
    if (version < 0) {
      throw new IllegalArgumentException("version must not be negative");
    }
    connections.requireTransaction(
        connection -> {
          try (PreparedStatement statement =
              connection.prepareStatement(
                  """
                  UPDATE eventdock.inbox_aggregate_versions
                     SET last_processed_version = ?, updated_at = ?
                   WHERE consumer_id = ? AND aggregate_type = ? AND aggregate_id = ?
                     AND last_processed_version <= ?
                  """)) {
            statement.setLong(1, version);
            statement.setObject(2, timestamp(now));
            statement.setString(3, key.consumerId());
            statement.setString(4, key.aggregateType());
            statement.setString(5, key.aggregateId());
            statement.setLong(6, version);
            requireOne(statement.executeUpdate(), "advance aggregate progress", key.toString());
          }
          return null;
        });
  }

  @Override
  public int deleteCompletedBefore(Instant processedBefore, int limit) {
    Objects.requireNonNull(processedBefore, "processedBefore must not be null");
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be positive");
    }
    return connections.inTransaction(
        connection -> {
          try (PreparedStatement statement =
              connection.prepareStatement(
                  """
                  WITH candidates AS (
                      SELECT consumer_id, event_id FROM eventdock.inbox_events
                       WHERE status IN ('PROCESSED', 'SKIPPED') AND processed_at < ?
                       ORDER BY processed_at LIMIT ? FOR UPDATE SKIP LOCKED
                  )
                  DELETE FROM eventdock.inbox_events AS inbox USING candidates
                   WHERE inbox.consumer_id = candidates.consumer_id
                     AND inbox.event_id = candidates.event_id
                  """)) {
            statement.setObject(1, timestamp(processedBefore));
            statement.setInt(2, limit);
            return statement.executeUpdate();
          }
        });
  }

  private void updateCompleted(
      String consumerId, EventId eventId, String status, String reason, Instant processedAt) {
    requireIdentity(consumerId, eventId);
    Objects.requireNonNull(processedAt, "processedAt must not be null");
    connections.withConnection(
        connection -> {
          try (PreparedStatement statement =
              connection.prepareStatement(
                  """
                  UPDATE eventdock.inbox_events
                     SET status = ?, processed_at = ?, locked_at = NULL, last_error = ?
                   WHERE consumer_id = ? AND event_id = ? AND status = 'PROCESSING'
                  """)) {
            statement.setString(1, status);
            statement.setObject(2, timestamp(processedAt));
            statement.setString(3, reason);
            statement.setString(4, consumerId);
            statement.setString(5, eventId.value());
            requireOne(statement.executeUpdate(), "complete inbox event", identity(consumerId, eventId));
          }
          return null;
        });
  }

  private void bindEvent(PreparedStatement statement, SerializedEvent event, int offset)
      throws SQLException {
    statement.setString(offset, event.id().value());
    statement.setString(offset + 1, event.type());
    statement.setInt(offset + 2, event.schemaVersion());
    statement.setString(offset + 3, event.aggregate().type());
    statement.setString(offset + 4, event.aggregate().id());
    statement.setLong(offset + 5, event.aggregate().version());
    statement.setObject(offset + 6, timestamp(event.occurredAt()));
    statement.setString(offset + 7, event.contentType());
    statement.setBytes(offset + 8, event.payload());
  }

  private InboxEntry toEntry(ResultSet result) throws SQLException {
    return new InboxEntry(
        result.getString("consumer_id"),
        JdbcValues.event(result, metadata),
        InboxStatus.valueOf(result.getString("status")),
        result.getInt("retry_count"),
        instant(result, "available_at"),
        instant(result, "locked_at"));
  }

  private void requireClaimArguments(int limit, Instant now, Duration lockTimeout) {
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be positive");
    }
    Objects.requireNonNull(now, "now must not be null");
    Objects.requireNonNull(lockTimeout, "lockTimeout must not be null");
    if (lockTimeout.isNegative() || lockTimeout.isZero()) {
      throw new IllegalArgumentException("lockTimeout must be positive");
    }
  }

  private void requireIdentity(String consumerId, EventId eventId) {
    requireConsumer(consumerId);
    Objects.requireNonNull(eventId, "eventId must not be null");
  }

  private void requireConsumer(String consumerId) {
    if (consumerId == null || consumerId.isBlank()) {
      throw new IllegalArgumentException("consumerId must not be blank");
    }
  }

  private String identity(String consumerId, EventId eventId) {
    return consumerId + "/" + eventId.value();
  }

  private String truncate(String reason) {
    return reason.length() <= 1000 ? reason : reason.substring(0, 1000);
  }
}
