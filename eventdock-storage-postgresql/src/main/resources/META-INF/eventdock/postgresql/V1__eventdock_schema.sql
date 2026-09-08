CREATE SCHEMA IF NOT EXISTS eventdock;

CREATE TABLE IF NOT EXISTS eventdock.outbox_events (
    event_id varchar(255) PRIMARY KEY,
    event_type varchar(255) NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    aggregate_type varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version >= 0),
    occurred_at timestamptz NOT NULL,
    content_type varchar(255) NOT NULL,
    payload bytea NOT NULL,
    metadata text NOT NULL DEFAULT '',
    status varchar(20) NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED')),
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_error varchar(1000),
    available_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_at timestamptz,
    published_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_eventdock_outbox_claim
    ON eventdock.outbox_events (available_at, occurred_at)
    WHERE status IN ('PENDING', 'PROCESSING');

CREATE INDEX IF NOT EXISTS idx_eventdock_outbox_cleanup
    ON eventdock.outbox_events (published_at)
    WHERE status = 'PUBLISHED';

CREATE TABLE IF NOT EXISTS eventdock.inbox_events (
    consumer_id varchar(255) NOT NULL,
    event_id varchar(255) NOT NULL,
    event_type varchar(255) NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    aggregate_type varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version >= 0),
    occurred_at timestamptz NOT NULL,
    content_type varchar(255) NOT NULL,
    payload bytea NOT NULL,
    metadata text NOT NULL DEFAULT '',
    status varchar(20) NOT NULL CHECK (status IN ('RECEIVED', 'PROCESSING', 'PROCESSED', 'SKIPPED', 'FAILED')),
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_error varchar(1000),
    available_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    locked_at timestamptz,
    processed_at timestamptz,
    PRIMARY KEY (consumer_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_eventdock_inbox_claim
    ON eventdock.inbox_events (consumer_id, available_at, received_at)
    WHERE status IN ('RECEIVED', 'PROCESSING');

CREATE INDEX IF NOT EXISTS idx_eventdock_inbox_cleanup
    ON eventdock.inbox_events (processed_at)
    WHERE status IN ('PROCESSED', 'SKIPPED');

CREATE TABLE IF NOT EXISTS eventdock.inbox_aggregate_versions (
    consumer_id varchar(255) NOT NULL,
    aggregate_type varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    last_processed_version bigint NOT NULL DEFAULT 0 CHECK (last_processed_version >= 0),
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (consumer_id, aggregate_type, aggregate_id)
);
