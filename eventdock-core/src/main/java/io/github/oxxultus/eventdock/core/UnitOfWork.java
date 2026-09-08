package io.github.oxxultus.eventdock.core;

import java.util.Objects;
import java.util.function.Supplier;

@FunctionalInterface
public interface UnitOfWork {
  <T> T execute(Supplier<T> action);

  default void execute(Runnable action) {
    Objects.requireNonNull(action, "action must not be null");
    execute(
        () -> {
          action.run();
          return null;
        });
  }

  static UnitOfWork direct() {
    return new UnitOfWork() {
      @Override
      public <T> T execute(Supplier<T> action) {
        return Objects.requireNonNull(action, "action must not be null").get();
      }
    };
  }
}
