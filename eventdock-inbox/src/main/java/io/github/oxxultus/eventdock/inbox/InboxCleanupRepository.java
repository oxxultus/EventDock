package io.github.oxxultus.eventdock.inbox;

import java.time.Instant;

@FunctionalInterface
public interface InboxCleanupRepository {
  int deleteCompletedBefore(Instant processedBefore, int limit);
}
