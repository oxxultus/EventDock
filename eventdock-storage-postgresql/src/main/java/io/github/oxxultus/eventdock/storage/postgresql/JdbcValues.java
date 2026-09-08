package io.github.oxxultus.eventdock.storage.postgresql;

import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

final class JdbcValues {
  private JdbcValues() {}

  static OffsetDateTime timestamp(Instant value) {
    return OffsetDateTime.ofInstant(value, ZoneOffset.UTC);
  }

  static Instant instant(ResultSet result, String column) throws SQLException {
    OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
    return value == null ? null : value.toInstant();
  }

  static SerializedEvent event(ResultSet result, MetadataCodec metadata) throws SQLException {
    return new SerializedEvent(
        new EventId(result.getString("event_id")),
        result.getString("event_type"),
        result.getInt("schema_version"),
        new AggregateRef(
            result.getString("aggregate_type"),
            result.getString("aggregate_id"),
            result.getLong("aggregate_version")),
        instant(result, "occurred_at"),
        result.getString("content_type"),
        result.getBytes("payload"),
        metadata.decode(result.getString("metadata")));
  }

  static void requireOne(int updated, String operation, String identity) {
    if (updated != 1) {
      throw new PostgresqlStorageException(
          operation + " expected one row but updated " + updated + ": " + identity);
    }
  }
}
