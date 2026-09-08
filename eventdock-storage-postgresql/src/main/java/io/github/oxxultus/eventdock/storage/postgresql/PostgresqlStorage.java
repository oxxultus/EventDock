package io.github.oxxultus.eventdock.storage.postgresql;

import java.util.Objects;
import javax.sql.DataSource;

public final class PostgresqlStorage {
  public static final String MIGRATION_RESOURCE =
      "META-INF/eventdock/postgresql/V1__eventdock_schema.sql";

  private final PostgresqlOutboxRepository outboxRepository;
  private final PostgresqlInboxRepository inboxRepository;
  private final PostgresqlUnitOfWork unitOfWork;

  public PostgresqlStorage(DataSource dataSource) {
    var connections = new JdbcConnectionContext(Objects.requireNonNull(dataSource));
    var metadata = new MetadataCodec();
    this.outboxRepository = new PostgresqlOutboxRepository(connections, metadata);
    this.inboxRepository = new PostgresqlInboxRepository(connections, metadata);
    this.unitOfWork = new PostgresqlUnitOfWork(connections);
  }

  public PostgresqlOutboxRepository outboxRepository() {
    return outboxRepository;
  }

  public PostgresqlInboxRepository inboxRepository() {
    return inboxRepository;
  }

  public PostgresqlUnitOfWork unitOfWork() {
    return unitOfWork;
  }
}
