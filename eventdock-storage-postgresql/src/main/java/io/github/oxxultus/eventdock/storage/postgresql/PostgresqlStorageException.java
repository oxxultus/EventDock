package io.github.oxxultus.eventdock.storage.postgresql;

public final class PostgresqlStorageException extends RuntimeException {
  public PostgresqlStorageException(String message, Throwable cause) {
    super(message, cause);
  }

  public PostgresqlStorageException(String message) {
    super(message);
  }
}
