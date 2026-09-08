package io.github.oxxultus.eventdock.outbox;

import java.time.Instant;

@FunctionalInterface
public interface OutboxCleanupRepository {
  int deletePublishedBefore(Instant publishedBefore, int limit);
}
