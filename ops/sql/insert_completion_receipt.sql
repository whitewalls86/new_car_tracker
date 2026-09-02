-- Durable proof that this generation completed against this manifest, so a
-- retried /coordination/complete can be answered from the receipt rather than
-- from the state row, which by then reads 'none' for every caller.
INSERT INTO coordination_completion_receipts
    (generation, manifest_sha256)
VALUES (%s, %s)
