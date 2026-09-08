package io.github.oxxultus.eventdock.core;

import java.util.Objects;

public final class DirectEventWriter implements EventWriter {
  private final EventCodec codec;
  private final EventPublisher publisher;

  public DirectEventWriter(EventCodec codec, EventPublisher publisher) {
    this.codec = Objects.requireNonNull(codec);
    this.publisher = Objects.requireNonNull(publisher);
  }

  @Override
  public void write(EventEnvelope<?> event) {
    publisher.publish(codec.encode(event));
  }
}
