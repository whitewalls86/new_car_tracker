-- Register a trained zstd dictionary (Plan 129 Stage 1).
-- Write-once by design: select_compression_dictionary_registration.sql refuses
-- ahead of this, because the bytes are immutable once any frame references
-- them. training_parameters and sample_keys are cast to jsonb explicitly.
INSERT INTO ops.compression_dictionaries (
    dict_id, version, minio_path, dictionary_bytes,
    dictionary_size_bytes, zstd_level, training_parameters,
    sample_keys, sample_sha256
) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
