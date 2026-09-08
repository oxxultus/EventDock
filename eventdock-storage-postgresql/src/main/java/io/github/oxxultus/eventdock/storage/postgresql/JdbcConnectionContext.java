package io.github.oxxultus.eventdock.storage.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;

final class JdbcConnectionContext {
  private final DataSource dataSource;
  private final ThreadLocal<Connection> current = new ThreadLocal<>();

  JdbcConnectionContext(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
  }

  <T> T withConnection(SqlFunction<Connection, T> action) {
    Connection existing = current.get();
    if (existing != null) {
      return apply(action, existing);
    }
    try (Connection connection = dataSource.getConnection()) {
      return action.apply(connection);
    } catch (SQLException exception) {
      throw failure("PostgreSQL operation failed", exception);
    }
  }

  <T> T inTransaction(SqlFunction<Connection, T> action) {
    Connection existing = current.get();
    if (existing != null) {
      return apply(action, existing);
    }
    try (Connection connection = dataSource.getConnection()) {
      boolean originalAutoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      current.set(connection);
      try {
        T result = action.apply(connection);
        connection.commit();
        return result;
      } catch (Throwable throwable) {
        rollback(connection, throwable);
        throw propagate(throwable);
      } finally {
        current.remove();
        connection.setAutoCommit(originalAutoCommit);
      }
    } catch (SQLException exception) {
      throw failure("PostgreSQL transaction failed", exception);
    }
  }

  <T> T requireTransaction(SqlFunction<Connection, T> action) {
    Connection connection = current.get();
    if (connection == null) {
      throw new PostgresqlStorageException("operation requires an active EventDock unit of work");
    }
    return apply(action, connection);
  }

  private <T> T apply(SqlFunction<Connection, T> action, Connection connection) {
    try {
      return action.apply(connection);
    } catch (SQLException exception) {
      throw failure("PostgreSQL operation failed", exception);
    }
  }

  private void rollback(Connection connection, Throwable original) {
    try {
      connection.rollback();
    } catch (SQLException rollbackFailure) {
      original.addSuppressed(rollbackFailure);
    }
  }

  private RuntimeException propagate(Throwable throwable) {
    if (throwable instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    return failure("PostgreSQL transaction failed", throwable);
  }

  private PostgresqlStorageException failure(String message, Throwable cause) {
    return new PostgresqlStorageException(message + ": " + cause.getMessage(), cause);
  }

  @FunctionalInterface
  interface SqlFunction<T, R> {
    R apply(T value) throws SQLException;
  }
}
