package io.github.oxxultus.eventdock.storage.postgresql;

import static io.github.oxxultus.eventdock.storage.postgresql.JdbcValues.instant;
import static io.github.oxxultus.eventdock.storage.postgresql.JdbcValues.requireOne;
import static io.github.oxxultus.eventdock.storage.postgresql.JdbcValues.timestamp;

import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import io.github.oxxultus.eventdock.outbox.OutboxEntry;
import io.github.oxxultus.eventdock.outbox.OutboxRepository;
import io.github.oxxultus.eventdock.outbox.OutboxStatus;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class PostgresqlOutboxRepository implements OutboxRepository {
  private static final String APPEND_SQL =
      """
      INSERT INTO eventdock.outbox_events (
          event_id, event_type, schema_version,
          aggregate_type, aggregate_id, aggregate_version,
          occurred_at, content_type, payload, metadata,
          status, retry_count, available_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, CURRENT_TIMESTAMP)
      """;

  private static final String CLAIM_SQL =
      """
      WITH candidates AS (
          SELECT event_id
            FROM eventdock.outbox_events
           WHERE (status = 'PENDING' AND available_at <= ?)
              OR (status = 'PROCESSING' AND locked_at < ?)
           ORDER BY available_at, occurred_at, event_id
           LIMIT ?
           FOR UPDATE SKIP LOCKED
      )
      UPDATE eventdock.outbox_events AS outbox
         SET status = 'PROCESSING', locked_at = ?
        FROM candidates
       WHERE outbox.event_id = candidates.event_id
      RETURNING outbox.*
      """;

  private final JdbcConnectionContext connections;
  private final MetadataCodec metadata;

  PostgresqlOutboxRepository(JdbcConnectionContext connections, MetadataCodec metadata) {
    this.connections = Objects.requireNonNull(connections);
    this.metadata = Objects.requireNonNull(metadata);
  }

  @Override
  public void append(SerializedEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    connections.withConnection(
        connection -> {
          try (PreparedStatement statement = connection.prepareStatement(APPEND_SQL)) {
            bindEvent(statement, event);
            statement.setString(10, metadata.encode(event.metadata()));
            requireOne(statement.executeUpdate(), "append outbox event", event.id().value());
          }
          return null;
        });
  }

  @Override
  public List<OutboxEntry> claim(int limit, Instant now, Duration lockTimeout) {
    requireClaimArguments(limit, now, lockTimeout);
    return connections.inTransaction(
        connection -> {
          try (PreparedStatement statement = connection.prepareStatement(CLAIM_SQL)) {
            statement.setObject(1, timestamp(now));
            statement.setObject(2, timestamp(now.minus(lockTimeout)));
            statement.setInt(3, limit);
            statement.setObject(4, timestamp(now));
            try (ResultSet result = statement.executeQuery()) {
              List<OutboxEntry> entries = new ArrayList<>();
              while (result.next()) {
                entries.add(toEntry(result));
              }
              return List.copyOf(entries);
            }
          }
        });
  }

  @Override
  public void markPublished(EventId eventId, Instant publishedAt) {
    updateState(
        """
        UPDATE eventdock.outbox_events
           SET status = 'PUBLISHED', published_at = ?, locked_at = NULL, last_error = NULL
         WHERE event_id = ? AND status = 'PROCESSING'
        """,
        eventId,
        publishedAt,
        "mark outbox event published");
  }

  @Override
  public void recordFailure(
      EventId eventId, String reason, Instant nextAttemptAt, boolean exhausted) {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(reason, "reason must not be null");
    Objects.requireNonNull(nextAttemptAt, "nextAttemptAt must not be null");
    connections.withConnection(
        connection -> {
          try (PreparedStatement statement =
              connection.prepareStatement(
                  """
                  UPDATE eventdock.outbox_events
                     SET status = ?, retry_count = retry_count + 1,
                         last_error = ?, available_at = ?, locked_at = NULL
                   WHERE event_id = ? AND status = 'PROCESSING'
                  """)) {
            statement.setString(1, exhausted ? "FAILED" : "PENDING");
            statement.setString(2, truncate(reason));
            statement.setObject(3, timestamp(nextAttemptAt));
            statement.setString(4, eventId.value());
            requireOne(statement.executeUpdate(), "record outbox failure", eventId.value());
          }
          return null;
        });
  }

  public int deletePublishedBefore(Instant publishedBefore, int limit) {
    Objects.requireNonNull(publishedBefore, "publishedBefore must not be null");
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be positive");
    }
    return connections.inTransaction(
        connection -> {
          try (PreparedStatement statement =
              connection.prepareStatement(
                  """
                  WITH candidates AS (
                      SELECT event_id FROM eventdock.outbox_events
                       WHERE status = 'PUBLISHED' AND published_at < ?
                       ORDER BY published_at LIMIT ? FOR UPDATE SKIP LOCKED
                  )
                  DELETE FROM eventdock.outbox_events AS outbox USING candidates
                   WHERE outbox.event_id = candidates.event_id
                  """)) {
            statement.setObject(1, timestamp(publishedBefore));
            statement.setInt(2, limit);
            return statement.executeUpdate();
          }
        });
  }

  private void updateState(
      String sql, EventId eventId, Instant timestampValue, String operation) {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(timestampValue, "timestamp must not be null");
    connections.withConnection(
        connection -> {
          try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, timestamp(timestampValue));
            statement.setString(2, eventId.value());
            requireOne(statement.executeUpdate(), operation, eventId.value());
          }
          return null;
        });
  }

  private void bindEvent(PreparedStatement statement, SerializedEvent event) throws SQLException {
    statement.setString(1, event.id().value());
    statement.setString(2, event.type());
    statement.setInt(3, event.schemaVersion());
    statement.setString(4, event.aggregate().type());
    statement.setString(5, event.aggregate().id());
    statement.setLong(6, event.aggregate().version());
    statement.setObject(7, timestamp(event.occurredAt()));
    statement.setString(8, event.contentType());
    statement.setBytes(9, event.payload());
  }

  private OutboxEntry toEntry(ResultSet result) throws SQLException {
    return new OutboxEntry(
        JdbcValues.event(result, metadata),
        OutboxStatus.valueOf(result.getString("status")),
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

  private String truncate(String reason) {
    return reason.length() <= 1000 ? reason : reason.substring(0, 1000);
  }
}
