-- Collision check before registering a trained zstd dictionary (Plan 129).
-- Either half being taken is a refusal: dict_id and version are both immutable
-- identities, and re-registering one would replace bytes that frames already
-- written against it still need to decompress.
SELECT dict_id, version
  FROM ops.compression_dictionaries
 WHERE dict_id = %s OR version = %s
