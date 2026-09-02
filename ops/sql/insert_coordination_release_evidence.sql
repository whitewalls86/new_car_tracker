-- Record one complete host-validation bundle against its generation.
-- gate_results and evidence_digests arrive as JSON text from json.dumps and are
-- cast here rather than adapted, so the statement is the only place that knows
-- the columns are jsonb.
INSERT INTO staging.coordination_release_evidence
    (generation, actor, gate_results, evidence_digests)
VALUES (%s, %s, %s::jsonb, %s::jsonb)
RETURNING evidence_id, submitted_at
