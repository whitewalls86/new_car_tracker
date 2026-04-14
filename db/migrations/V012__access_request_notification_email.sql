-- Opt-in email notification for access request outcomes.
-- Stored only if the requester checks the notification opt-in.
-- Nulled immediately after notification is sent, or after 48 hours (safety net).

ALTER TABLE access_requests ADD COLUMN IF NOT EXISTS notification_email TEXT;
