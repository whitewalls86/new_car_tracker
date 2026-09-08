-- How many rows a dbt model materialized.
--
-- Plan 162 Stage S. The relation name is generated -- the gate walks every
-- model under dbt/models/ rather than naming any of them, because a list of
-- models beside the models is the kind of thing that goes stale and lets a new
-- model escape the obligation. That is what makes this a template.
--
-- No braces in this comment beyond the placeholder: the whole file goes
-- through str.format.
SELECT count(*) FROM {relation}
