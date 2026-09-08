package io.github.oxxultus.eventdock.autoconfigure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("eventdock")
public class EventDockProperties {
  private final Processing outbox = new Processing();
  private final Inbox inbox = new Inbox();
  private final Retry retry = new Retry();
  private final Cleanup cleanup = new Cleanup();
  private Duration publishTimeout = Duration.ofSeconds(10);

  public Processing getOutbox() { return outbox; }
  public Inbox getInbox() { return inbox; }
  public Retry getRetry() { return retry; }
  public Cleanup getCleanup() { return cleanup; }
  public Duration getPublishTimeout() { return publishTimeout; }
  public void setPublishTimeout(Duration value) { publishTimeout = value; }

  public static class Processing {
    private boolean enabled = true;
    private int batchSize = 100;
    private Duration pollInterval = Duration.ofSeconds(1);

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean value) { enabled = value; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int value) { batchSize = value; }
    public Duration getPollInterval() { return pollInterval; }
    public void setPollInterval(Duration value) { pollInterval = value; }
  }

  public static final class Inbox extends Processing {
    private String consumerId;
    private List<String> topics = new ArrayList<>();

    public Inbox() {
      setEnabled(false);
    }

    public String getConsumerId() { return consumerId; }
    public void setConsumerId(String value) { consumerId = value; }
    public List<String> getTopics() { return topics; }
    public void setTopics(List<String> value) { topics = value; }
  }

  public static final class Retry {
    private int maxAttempts = 5;
    private Duration initialDelay = Duration.ofSeconds(1);
    private Duration maxDelay = Duration.ofMinutes(1);
    private Duration lockTimeout = Duration.ofMinutes(1);

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int value) { maxAttempts = value; }
    public Duration getInitialDelay() { return initialDelay; }
    public void setInitialDelay(Duration value) { initialDelay = value; }
    public Duration getMaxDelay() { return maxDelay; }
    public void setMaxDelay(Duration value) { maxDelay = value; }
    public Duration getLockTimeout() { return lockTimeout; }
    public void setLockTimeout(Duration value) { lockTimeout = value; }
  }

  public static final class Cleanup {
    private boolean enabled = true;
    private Duration interval = Duration.ofHours(1);
    private Duration outboxRetention = Duration.ofDays(7);
    private Duration inboxRetention = Duration.ofDays(30);
    private int batchSize = 1000;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean value) { enabled = value; }
    public Duration getInterval() { return interval; }
    public void setInterval(Duration value) { interval = value; }
    public Duration getOutboxRetention() { return outboxRetention; }
    public void setOutboxRetention(Duration value) { outboxRetention = value; }
    public Duration getInboxRetention() { return inboxRetention; }
    public void setInboxRetention(Duration value) { inboxRetention = value; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int value) { batchSize = value; }
  }

  public void validate() {
    positive("publish-timeout", publishTimeout);
    positive("outbox.batch-size", outbox.getBatchSize());
    positive("outbox.poll-interval", outbox.getPollInterval());
    positive("retry.max-attempts", retry.getMaxAttempts());
    positive("retry.initial-delay", retry.getInitialDelay());
    positive("retry.max-delay", retry.getMaxDelay());
    positive("retry.lock-timeout", retry.getLockTimeout());
    if (retry.getMaxDelay().compareTo(retry.getInitialDelay()) < 0) {
      throw new IllegalStateException("eventdock.retry.max-delay must not be shorter than initial-delay");
    }
    positive("cleanup.interval", cleanup.getInterval());
    positive("cleanup.outbox-retention", cleanup.getOutboxRetention());
    positive("cleanup.inbox-retention", cleanup.getInboxRetention());
    positive("cleanup.batch-size", cleanup.getBatchSize());
    if (inbox.isEnabled()) {
      if (inbox.getConsumerId() == null || inbox.getConsumerId().isBlank()) {
        throw new IllegalStateException("eventdock.inbox.consumer-id is required when inbox is enabled");
      }
      if (inbox.getTopics() == null || inbox.getTopics().isEmpty()) {
        throw new IllegalStateException("eventdock.inbox.topics is required when inbox is enabled");
      }
      positive("inbox.batch-size", inbox.getBatchSize());
      positive("inbox.poll-interval", inbox.getPollInterval());
    }
  }

  private void positive(String name, Duration value) {
    if (value == null || value.isZero() || value.isNegative()) {
      throw new IllegalStateException("eventdock." + name + " must be positive");
    }
  }

  private void positive(String name, int value) {
    if (value < 1) {
      throw new IllegalStateException("eventdock." + name + " must be positive");
    }
  }
}
