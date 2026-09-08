package io.github.oxxultus.eventdock.core;

import java.time.Duration;

public interface RetryPolicy {
  Duration lockTimeout();

  boolean exhausted(int attemptCount);

  Duration delayAfter(int attemptCount);
}
