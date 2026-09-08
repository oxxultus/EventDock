package io.github.oxxultus.eventdock.inbox;

@FunctionalInterface
public interface InboxExhaustionHandler {
  void onExhausted(InboxEntry entry, RuntimeException cause);

  static InboxExhaustionHandler noOp() {
    return (entry, cause) -> {};
  }
}
