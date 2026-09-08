package io.github.oxxultus.eventdock.core;

import java.util.Objects;

public record AggregateRef(String type, String id, long version) {
  public AggregateRef {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(id, "id must not be null");
    if (type.isBlank() || id.isBlank()) {
      throw new IllegalArgumentException("type and id must not be blank");
    }
    if (version < 0) {
      throw new IllegalArgumentException("version must not be negative");
    }
  }
}
