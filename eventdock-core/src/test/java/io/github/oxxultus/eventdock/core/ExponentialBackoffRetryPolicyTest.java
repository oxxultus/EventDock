package io.github.oxxultus.eventdock.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ExponentialBackoffRetryPolicyTest {
  private final ExponentialBackoffRetryPolicy policy =
      new ExponentialBackoffRetryPolicy(
          3, Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(30));

  @Test
  void increasesDelayExponentiallyAndCapsMaximum() {
    assertEquals(Duration.ofSeconds(2), policy.delayAfter(1));
    assertEquals(Duration.ofSeconds(4), policy.delayAfter(2));
    assertEquals(Duration.ofSeconds(5), policy.delayAfter(3));
  }

  @Test
  void exhaustsAtConfiguredAttemptCount() {
    assertFalse(policy.exhausted(2));
    assertTrue(policy.exhausted(3));
  }
}
