-- Plan 82: store requester display name on access_requests for admin review
ALTER TABLE access_requests ADD COLUMN IF NOT EXISTS display_name TEXT;
