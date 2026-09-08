package io.github.oxxultus.eventdock.outbox;

import io.github.oxxultus.eventdock.core.EventCodec;
import io.github.oxxultus.eventdock.core.EventEnvelope;
import io.github.oxxultus.eventdock.core.EventWriter;
import java.util.Objects;

public final class OutboxWriter implements EventWriter {
  private final OutboxRepository repository;
  private final EventCodec codec;

  public OutboxWriter(OutboxRepository repository, EventCodec codec) {
    this.repository = Objects.requireNonNull(repository);
    this.codec = Objects.requireNonNull(codec);
  }

  public void append(EventEnvelope<?> event) {
    repository.append(codec.encode(event));
  }

  @Override
  public void write(EventEnvelope<?> event) {
    append(event);
  }
}
