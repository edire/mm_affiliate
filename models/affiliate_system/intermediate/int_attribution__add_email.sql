
{{ config(
    materialized = 'table'
)}}

WITH email_attribution_rank as (
  SELECT c.*
    , e.*
    , ROW_NUMBER() OVER ( PARTITION BY c.event_id ORDER BY e.e_pv_collector_tstamp DESC) AS email_idx
  FROM {{ ref('int_attribution__join') }} c
  LEFT JOIN {{ ref('int_attribution__email') }} e
    ON c.user_id = e.e_user_id
    AND c.collector_tstamp >= e.e_pv_collector_tstamp
)

SELECT *
FROM email_attribution_rank