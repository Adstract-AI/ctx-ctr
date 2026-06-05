CREATE TABLE IF NOT EXISTS ctr_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL CHECK (event_type IN ('impression', 'click')),
    ad_category TEXT NOT NULL,
    publisher_domain TEXT NOT NULL,
    conversation_category TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ctr_events_bucket_time
    ON ctr_events (
        ad_category,
        publisher_domain,
        conversation_category,
        occurred_at
    );

CREATE TABLE IF NOT EXISTS ctr_bucket_statistics (
    ad_category TEXT NOT NULL,
    publisher_domain TEXT NOT NULL,
    conversation_category TEXT NOT NULL,
    impressions BIGINT NOT NULL DEFAULT 0 CHECK (impressions >= 0),
    clicks BIGINT NOT NULL DEFAULT 0 CHECK (clicks >= 0),
    alpha_prior DOUBLE PRECISION NOT NULL,
    beta_prior DOUBLE PRECISION NOT NULL,
    alpha_posterior DOUBLE PRECISION NOT NULL,
    beta_posterior DOUBLE PRECISION NOT NULL,
    ctr DOUBLE PRECISION NOT NULL,
    variance DOUBLE PRECISION NOT NULL,
    ci_low DOUBLE PRECISION NOT NULL,
    ci_high DOUBLE PRECISION NOT NULL,
    trusted BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (
        ad_category,
        publisher_domain,
        conversation_category
    ),
    CHECK (clicks <= impressions)
);

CREATE TABLE IF NOT EXISTS ctr_model_snapshots (
    id BIGSERIAL PRIMARY KEY,
    snapshot_name TEXT NOT NULL,
    w0 DOUBLE PRECISION NOT NULL,
    weights JSONB NOT NULL,
    metrics JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ctr_experiment_results (
    id BIGSERIAL PRIMARY KEY,
    experiment_name TEXT NOT NULL,
    config JSONB NOT NULL,
    metrics JSONB NOT NULL,
    artifact_uri TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
