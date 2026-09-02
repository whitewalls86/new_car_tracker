-- Serialise every coordination state mutation on one transaction-scoped
-- advisory lock (ops.routers.coordination.COORDINATION_LOCK_ID = 142). It is
-- released when the transaction ends, so no path has to unlock it explicitly.
SELECT pg_advisory_xact_lock(%s)
