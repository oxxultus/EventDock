package io.github.oxxultus.eventdock.outbox;

import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public interface OutboxRepository {
  void append(SerializedEvent event);

  List<OutboxEntry> claim(int limit, Instant now, Duration lockTimeout);

  void markPublished(EventId eventId, Instant publishedAt);

  void recordFailure(EventId eventId, String reason, Instant nextAttemptAt, boolean exhausted);
}
