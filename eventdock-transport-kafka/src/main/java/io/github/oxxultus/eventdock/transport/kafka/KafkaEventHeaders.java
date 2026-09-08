package io.github.oxxultus.eventdock.transport.kafka;

public final class KafkaEventHeaders {
  public static final String EVENT_ID = "eventdock-event-id";
  public static final String EVENT_TYPE = "eventdock-event-type";
  public static final String SCHEMA_VERSION = "eventdock-schema-version";
  public static final String AGGREGATE_TYPE = "eventdock-aggregate-type";
  public static final String AGGREGATE_ID = "eventdock-aggregate-id";
  public static final String AGGREGATE_VERSION = "eventdock-aggregate-version";
  public static final String OCCURRED_AT = "eventdock-occurred-at";
  public static final String CONTENT_TYPE = "eventdock-content-type";
  public static final String METADATA_PREFIX = "eventdock-metadata-";

  private KafkaEventHeaders() {}
}
