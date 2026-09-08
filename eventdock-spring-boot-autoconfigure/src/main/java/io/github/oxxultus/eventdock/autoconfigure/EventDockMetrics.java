package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.inbox.InboxProcessingResult;
import io.github.oxxultus.eventdock.outbox.OutboxProcessingResult;

public interface EventDockMetrics {
  void recordOutbox(OutboxProcessingResult result);

  void recordInbox(InboxProcessingResult result);

  void recordCleanup(int outboxDeleted, int inboxDeleted);

  static EventDockMetrics noOp() {
    return new EventDockMetrics() {
      @Override
      public void recordOutbox(OutboxProcessingResult result) {}

      @Override
      public void recordInbox(InboxProcessingResult result) {}

      @Override
      public void recordCleanup(int outboxDeleted, int inboxDeleted) {}
    };
  }
}
