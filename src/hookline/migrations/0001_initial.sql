-- Stage 2: the queue is a table. One delivery row per event x endpoint.

CREATE TABLE apps (
    id         uuid PRIMARY KEY,
    name       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE endpoints (
    id         uuid PRIMARY KEY,
    app_id     uuid NOT NULL REFERENCES apps (id),
    url        text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX endpoints_app_idx ON endpoints (app_id);

CREATE TABLE events (
    id         uuid PRIMARY KEY,
    app_id     uuid NOT NULL REFERENCES apps (id),
    type       text NOT NULL,
    payload    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE deliveries (
    id          bigserial PRIMARY KEY,
    event_id    uuid NOT NULL REFERENCES events (id),
    endpoint_id uuid NOT NULL REFERENCES endpoints (id),
    status      text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'delivering', 'delivered', 'failed')),
    attempts    int NOT NULL DEFAULT 0,
    last_status int,
    last_error  text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX deliveries_event_idx ON deliveries (event_id);
-- what the worker polls: oldest pending first
CREATE INDEX deliveries_pending_idx ON deliveries (id) WHERE status = 'pending';
