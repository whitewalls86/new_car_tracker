-- Every host-validation bundle recorded for one generation, newest first. The
-- caller releases if any single bundle passes all of HOST_VALIDATION_GATES, so
-- the ordering is for reporting rather than for the decision.
SELECT gate_results
  FROM staging.coordination_release_evidence
 WHERE generation = %s
 ORDER BY evidence_id DESC
