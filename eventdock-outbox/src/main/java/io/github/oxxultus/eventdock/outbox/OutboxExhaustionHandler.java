package io.github.oxxultus.eventdock.outbox;

@FunctionalInterface
public interface OutboxExhaustionHandler {
  void onExhausted(OutboxEntry entry, RuntimeException cause);

  static OutboxExhaustionHandler noOp() {
    return (entry, cause) -> {};
  }
}
