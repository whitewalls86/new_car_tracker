-- Plan 129: immutable registry for zstd dictionaries used by bronze HTML.
CREATE TABLE ops.compression_dictionaries (
    -- bigint, not integer: zstd masks generated dictionary IDs to 31 bits, so
    -- observed values run to ~2.09e9 against integer's 2147483647 ceiling --
    -- it fits, but with no headroom, and an explicitly chosen ID above 2^31
    -- would fail to insert. bigint costs nothing here.
    dict_id bigint PRIMARY KEY CHECK (dict_id > 0),
    version integer NOT NULL UNIQUE CHECK (version > 0),
    minio_path text NOT NULL UNIQUE,
    dictionary_bytes bytea NOT NULL,
    dictionary_size_bytes integer NOT NULL CHECK (dictionary_size_bytes > 0),
    zstd_level integer NOT NULL,
    training_parameters jsonb NOT NULL,
    sample_keys jsonb NOT NULL,
    sample_sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (jsonb_typeof(sample_keys) = 'array')
);

COMMENT ON TABLE ops.compression_dictionaries IS
    'Immutable recovery/provenance registry for bronze HTML zstd dictionaries; rows must never be deleted.';

ALTER TABLE ops.compression_dictionaries OWNER TO dbt_user;
GRANT SELECT ON ops.compression_dictionaries TO viewer;
GRANT SELECT ON ops.compression_dictionaries TO scraper_user;
