package io.github.oxxultus.eventdock.outbox;

import java.time.Duration;

public interface RetryPolicy {
  Duration lockTimeout();

  boolean exhausted(int attemptCount);

  Duration delayAfter(int attemptCount);
}
