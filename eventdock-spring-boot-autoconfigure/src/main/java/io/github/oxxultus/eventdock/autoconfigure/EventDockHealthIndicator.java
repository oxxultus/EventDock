package io.github.oxxultus.eventdock.autoconfigure;

import java.sql.ResultSet;
import javax.sql.DataSource;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.jdbc.core.JdbcTemplate;

public final class EventDockHealthIndicator implements HealthIndicator {
  private static final String STATUS_SQL =
      """
      SELECT
        (SELECT count(*) FROM eventdock.outbox_events WHERE status IN ('PENDING', 'PROCESSING')),
        (SELECT count(*) FROM eventdock.outbox_events WHERE status = 'FAILED'),
        (SELECT count(*) FROM eventdock.inbox_events WHERE status IN ('RECEIVED', 'PROCESSING')),
        (SELECT count(*) FROM eventdock.inbox_events WHERE status = 'FAILED')
      """;

  private final JdbcTemplate jdbc;

  public EventDockHealthIndicator(DataSource dataSource) {
    this.jdbc = new JdbcTemplate(dataSource);
  }

  @Override
  public Health health() {
    try {
      long[] counts =
          jdbc.queryForObject(
              STATUS_SQL,
              (ResultSet result, int row) ->
                  new long[] {
                    result.getLong(1), result.getLong(2), result.getLong(3), result.getLong(4)
                  });
      return Health.up()
          .withDetail("outboxPending", counts[0])
          .withDetail("outboxFailed", counts[1])
          .withDetail("inboxPending", counts[2])
          .withDetail("inboxFailed", counts[3])
          .build();
    } catch (RuntimeException exception) {
      return Health.down(exception).build();
    }
  }
}
