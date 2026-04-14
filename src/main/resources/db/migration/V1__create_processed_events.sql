CREATE TABLE processed_events (
    id          BIGSERIAL    PRIMARY KEY,
    order_id    VARCHAR(36)  NOT NULL UNIQUE,
    processed_at TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_events_order_id ON processed_events (order_id);
