package io.github.oxxultus.eventdock.core;

import java.util.Objects;
import java.util.UUID;

public record EventId(String value) {
  public EventId {
    Objects.requireNonNull(value, "value must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException("value must not be blank");
    }
  }

  public static EventId random() {
    return new EventId(UUID.randomUUID().toString());
  }
}
