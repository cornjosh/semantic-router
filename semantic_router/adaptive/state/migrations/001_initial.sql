CREATE TABLE IF NOT EXISTS schema_version (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version integer NOT NULL
);
INSERT INTO schema_version (version) VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS decisions (
    decision_id uuid PRIMARY KEY,
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    fingerprint text NOT NULL,
    cluster_id text NOT NULL,
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    terminal boolean NOT NULL DEFAULT false,
    payload jsonb NOT NULL,
    UNIQUE (tenant_id, request_id)
);
CREATE INDEX IF NOT EXISTS decisions_pending ON decisions (cluster_id, expires_at) WHERE NOT terminal;
CREATE INDEX IF NOT EXISTS decisions_latest ON decisions (cluster_id, created_at DESC);

CREATE TABLE IF NOT EXISTS events (
    tenant_id text NOT NULL,
    event_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('observation', 'feedback')),
    decision_id uuid NOT NULL REFERENCES decisions(decision_id),
    fingerprint text NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (tenant_id, event_id),
    UNIQUE (kind, decision_id)
);

CREATE TABLE IF NOT EXISTS quality_counts (
    tenant_id text NOT NULL,
    policy_version text NOT NULL,
    feature_version text NOT NULL,
    category text NOT NULL,
    difficulty text NOT NULL,
    model_id text NOT NULL,
    model_version text NOT NULL,
    successes bigint NOT NULL DEFAULT 0 CHECK (successes >= 0),
    failures bigint NOT NULL DEFAULT 0 CHECK (failures >= 0),
    PRIMARY KEY (tenant_id, policy_version, feature_version, category, difficulty, model_id, model_version)
);

CREATE TABLE IF NOT EXISTS cluster_stats (
    cluster_id text PRIMARY KEY,
    payload jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS polling_leases (
    cluster_id text PRIMARY KEY,
    owner text NOT NULL,
    expires_at timestamptz NOT NULL
);
