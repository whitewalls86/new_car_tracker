SELECT locked,
       locked_at AT TIME ZONE 'America/Chicago' AS locked_at,
       locked_by
FROM dbt_lock
WHERE id = 1
