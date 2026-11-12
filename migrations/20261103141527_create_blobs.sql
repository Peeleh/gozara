-- Add migration script here
CREATE TYPE status AS ENUM (
    'gathering',
    'distributing',
    'finished',
    'failed'
);

CREATE TABLE blobs (
    id TEXT PRIMARY KEY,
    status status NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
