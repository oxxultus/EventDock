package io.github.oxxultus.eventdock.inbox;

import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public interface InboxRepository {
  boolean receive(String consumerId, SerializedEvent event, Instant receivedAt);

  List<InboxEntry> claim(String consumerId, int limit, Instant now, Duration lockTimeout);

  void markProcessed(String consumerId, EventId eventId, Instant processedAt);

  void markSkipped(String consumerId, EventId eventId, String reason, Instant processedAt);

  void recordFailure(
      String consumerId, EventId eventId, String reason, Instant nextAttemptAt, boolean exhausted);
}
