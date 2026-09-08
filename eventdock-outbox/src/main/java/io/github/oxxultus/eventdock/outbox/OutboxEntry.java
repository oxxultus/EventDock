package io.github.oxxultus.eventdock.outbox;

import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.time.Instant;

public record OutboxEntry(
    SerializedEvent event,
    OutboxStatus status,
    int attemptCount,
    Instant availableAt,
    Instant lockedAt) {}
