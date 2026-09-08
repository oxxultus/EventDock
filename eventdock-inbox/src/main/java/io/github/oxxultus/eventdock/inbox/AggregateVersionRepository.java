package io.github.oxxultus.eventdock.inbox;

import java.time.Instant;

public interface AggregateVersionRepository {
  long getOrCreateAndLock(AggregateKey key, Instant now);

  void advance(AggregateKey key, long version, Instant now);
}
