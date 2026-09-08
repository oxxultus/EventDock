package io.github.oxxultus.eventdock.core;

import java.time.Duration;
import java.util.Objects;

public record ExponentialBackoffRetryPolicy(
    int maxAttempts, Duration initialDelay, Duration maxDelay, Duration lockTimeout)
    implements RetryPolicy {

  public ExponentialBackoffRetryPolicy {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be positive");
    }
    Objects.requireNonNull(initialDelay, "initialDelay must not be null");
    Objects.requireNonNull(maxDelay, "maxDelay must not be null");
    Objects.requireNonNull(lockTimeout, "lockTimeout must not be null");
    if (initialDelay.isNegative() || initialDelay.isZero()) {
      throw new IllegalArgumentException("initialDelay must be positive");
    }
    if (maxDelay.compareTo(initialDelay) < 0) {
      throw new IllegalArgumentException("maxDelay must not be shorter than initialDelay");
    }
    if (lockTimeout.isNegative() || lockTimeout.isZero()) {
      throw new IllegalArgumentException("lockTimeout must be positive");
    }
  }

  @Override
  public boolean exhausted(int attemptCount) {
    return attemptCount >= maxAttempts;
  }

  @Override
  public Duration delayAfter(int attemptCount) {
    if (attemptCount < 1) {
      throw new IllegalArgumentException("attemptCount must be positive");
    }
    Duration delay = initialDelay;
    for (int attempt = 1; attempt < attemptCount && delay.compareTo(maxDelay) < 0; attempt++) {
      try {
        delay = delay.multipliedBy(2);
      } catch (ArithmeticException ignored) {
        return maxDelay;
      }
    }
    return delay.compareTo(maxDelay) > 0 ? maxDelay : delay;
  }
}
