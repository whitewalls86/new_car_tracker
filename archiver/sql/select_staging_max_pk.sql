-- Snapshot boundary for one staging-events flush.
-- The relation and its primary key are filled in from _TABLE_CONFIGS in
-- archiver/processors/flush_staging_events.py, which is the whole reason this
-- statement is a template: seven staging tables share one flush, and the only
-- thing that varies between them is those two names. Both are identifiers, so
-- neither can be a bind parameter; both are repository constants, never
-- request input.
SELECT MAX({pk}) FROM {table}
