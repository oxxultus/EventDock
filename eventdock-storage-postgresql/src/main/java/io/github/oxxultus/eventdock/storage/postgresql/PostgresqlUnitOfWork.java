package io.github.oxxultus.eventdock.storage.postgresql;

import io.github.oxxultus.eventdock.core.UnitOfWork;
import java.util.Objects;
import java.util.function.Supplier;

public final class PostgresqlUnitOfWork implements UnitOfWork {
  private final JdbcConnectionContext connections;

  PostgresqlUnitOfWork(JdbcConnectionContext connections) {
    this.connections = Objects.requireNonNull(connections);
  }

  @Override
  public <T> T execute(Supplier<T> action) {
    Objects.requireNonNull(action, "action must not be null");
    return connections.inTransaction(connection -> action.get());
  }
}
