-- Plan 72: add archived_at to raw_artifacts
--
-- Lifecycle semantics after this migration:
--   archived_at  — HTML archived to MinIO Parquet; disk file deleted
--   deleted_at   — Parquet partition purged from MinIO (28 days after archived_at)

ALTER TABLE public.raw_artifacts
    ADD COLUMN IF NOT EXISTS archived_at timestamp with time zone;
