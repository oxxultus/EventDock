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
  private Duration publishTimeout = Duration.ofSeconds(10);

  public Processing getOutbox() { return outbox; }
  public Inbox getInbox() { return inbox; }
  public Retry getRetry() { return retry; }
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
}
