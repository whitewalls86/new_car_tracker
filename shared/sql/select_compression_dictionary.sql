-- Fetch a trained zstd dictionary by id (Plan 129).
-- minio_path is where the dictionary bytes live when they are not inlined;
-- dictionary_bytes is the inline copy, which is what the decompressor uses.
SELECT minio_path, dictionary_bytes
  FROM ops.compression_dictionaries
 WHERE dict_id = %s
