package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.core.ConsumerMode;
import io.github.oxxultus.eventdock.core.ProducerMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("eventdock")
public class EventDockProperties {
  private final Processing outbox = new Processing();
  private final Inbox inbox = new Inbox();
  private final Producer producer = new Producer();
  private final Consumer consumer = new Consumer();
  private final Retry retry = new Retry();
  private final Cleanup cleanup = new Cleanup();
  private List<ConsumerBinding> consumers = new ArrayList<>();
  private Duration publishTimeout = Duration.ofSeconds(10);

  public Processing getOutbox() { return outbox; }
  public Inbox getInbox() { return inbox; }
  public Producer getProducer() { return producer; }
  public Consumer getConsumer() { return consumer; }
  public Retry getRetry() { return retry; }
  public Cleanup getCleanup() { return cleanup; }
  public List<ConsumerBinding> getConsumers() { return consumers; }
  public void setConsumers(List<ConsumerBinding> value) { consumers = value; }
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

  public static final class Producer {
    private ProducerMode mode = ProducerMode.OUTBOX;

    public ProducerMode getMode() { return mode; }
    public void setMode(ProducerMode value) { mode = value; }
  }

  public static final class Consumer {
    private ConsumerMode mode = ConsumerMode.INBOX;

    public ConsumerMode getMode() { return mode; }
    public void setMode(ConsumerMode value) { mode = value; }
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

  public static final class ConsumerBinding {
    private String id;
    private ConsumerMode mode = ConsumerMode.INBOX;
    private List<String> topics = new ArrayList<>();
    private final Kafka kafka = new Kafka();
    private List<Route> routes = new ArrayList<>();

    public String getId() { return id; }
    public void setId(String value) { id = value; }
    public ConsumerMode getMode() { return mode; }
    public void setMode(ConsumerMode value) { mode = value; }
    public List<String> getTopics() { return topics; }
    public void setTopics(List<String> value) { topics = value; }
    public Kafka getKafka() { return kafka; }
    public List<Route> getRoutes() { return routes; }
    public void setRoutes(List<Route> value) { routes = value; }

    public String groupId() {
      return kafka.getGroupId() == null || kafka.getGroupId().isBlank()
          ? id
          : kafka.getGroupId();
    }

    public String consumerId(String eventType) {
      return routes.stream()
          .filter(route -> route.getEventType().equals(eventType))
          .map(Route::getConsumerId)
          .findFirst()
          .orElse(id);
    }
  }

  public static final class Kafka {
    private String groupId;

    public String getGroupId() { return groupId; }
    public void setGroupId(String value) { groupId = value; }
  }

  public static final class Route {
    private String eventType;
    private String consumerId;

    public String getEventType() { return eventType; }
    public void setEventType(String value) { eventType = value; }
    public String getConsumerId() { return consumerId; }
    public void setConsumerId(String value) { consumerId = value; }
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
    if (producer.getMode() == null) {
      throw new IllegalStateException("eventdock.producer.mode must not be null");
    }
    if (consumer.getMode() == null) {
      throw new IllegalStateException("eventdock.consumer.mode must not be null");
    }
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
    validateConsumers();
  }

  private void validateConsumers() {
    if (consumers == null) {
      throw new IllegalStateException("eventdock.consumers must not be null");
    }
    if (!consumers.isEmpty()
        && (inbox.isEnabled()
            || (inbox.getConsumerId() != null && !inbox.getConsumerId().isBlank())
            || (inbox.getTopics() != null && !inbox.getTopics().isEmpty()))) {
      throw new IllegalStateException(
          "eventdock.consumers cannot be combined with legacy eventdock.inbox consumer settings");
    }
    var ids = new java.util.HashSet<String>();
    for (int index = 0; index < consumers.size(); index++) {
      ConsumerBinding binding = consumers.get(index);
      String prefix = "eventdock.consumers[" + index + "]";
      required(prefix + ".id", binding.getId());
      if (!ids.add(binding.getId())) {
        throw new IllegalStateException("duplicate eventdock consumer id: " + binding.getId());
      }
      if (binding.getMode() == null) {
        throw new IllegalStateException(prefix + ".mode must not be null");
      }
      if (binding.getTopics() == null || binding.getTopics().isEmpty()) {
        throw new IllegalStateException(prefix + ".topics is required");
      }
      binding.getTopics().forEach(topic -> required(prefix + ".topics", topic));
      required(prefix + ".kafka.group-id", binding.groupId());
      if (binding.getRoutes() == null) {
        throw new IllegalStateException(prefix + ".routes must not be null");
      }
      var eventTypes = new java.util.HashSet<String>();
      for (Route route : binding.getRoutes()) {
        required(prefix + ".routes.event-type", route.getEventType());
        required(prefix + ".routes.consumer-id", route.getConsumerId());
        if (!eventTypes.add(route.getEventType())) {
          throw new IllegalStateException(
              "duplicate route event-type in " + binding.getId() + ": " + route.getEventType());
        }
      }
    }
  }

  private void required(String name, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(name + " must not be blank");
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
