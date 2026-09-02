-- Stamp one legacy (slot-less) config as queued, keyed by search_key. The
-- slot path uses mark_rotation_slot_queued.sql, which stamps a whole slot.
UPDATE search_configs SET last_queued_at = now() WHERE search_key = %s
