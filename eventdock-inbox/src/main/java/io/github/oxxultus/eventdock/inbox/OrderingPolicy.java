package io.github.oxxultus.eventdock.inbox;

public enum OrderingPolicy {
  IDEMPOTENT_ONLY,
  IGNORE_OLD_VERSIONS,
  STRICT_AGGREGATE_ORDER
}
