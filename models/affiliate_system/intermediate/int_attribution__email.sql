
{{ config(
    materialized = 'table'
)}}

WITH email_attribution as (
  SELECT user_id as e_user_id
    , new_affid_attribution as e_pv_affiliate_id
    , new_subid_attribution as e_pv_sub_id
    , collector_tstamp AS e_pv_collector_tstamp
    , event_id as e_pv_event_id
  FROM {{ ref('int_attribution__join') }}
  WHERE new_affid_attribution IS NOT NULL
    AND new_affid_attribution <> '0'
)

SELECT *
FROM email_attribution