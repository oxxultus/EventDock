package io.github.oxxultus.eventdock.inbox;

import io.github.oxxultus.eventdock.core.SerializedEvent;
import java.time.Instant;

public record InboxEntry(
    String consumerId,
    SerializedEvent event,
    InboxStatus status,
    int attemptCount,
    Instant availableAt,
    Instant lockedAt) {}
