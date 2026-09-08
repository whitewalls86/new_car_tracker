-- Whether this deployment holds any usable machine credential at all.
--
-- A different question from resolving one, producing a different status: "this
-- deployment has no tokens" is an operator's 503, "your token is wrong" is the
-- caller's 403. Collapsing them blames whichever party did nothing wrong.
--
-- Active means issued, not revoked, and not past its expiry -- the same three
-- conditions the resolver applies. A deployment whose only credential expired
-- overnight then reports itself unconfigured, which is true, rather than
-- refusing every caller as though each had presented a bad token.
SELECT EXISTS (
    SELECT 1
    FROM ops.machine_tokens
    WHERE revoked_at IS NULL
      AND (expires_at IS NULL OR expires_at > now())
) AS configured
