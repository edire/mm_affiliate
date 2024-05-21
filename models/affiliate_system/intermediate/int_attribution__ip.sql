
{{ config(
    materialized = 'table'
)}}

WITH ip_attribution_rank as (
  SELECT c.*
    , i.user_ipaddress AS i_pv_user_ipaddress
    , i.affiliate_id AS i_pv_affiliate_id	
    , i.sub_id AS i_pv_sub_id	
    , i.collector_tstamp AS i_pv_collector_tstamp	
    , i.event_id AS i_pv_event_id
    , i.page_urlhost as i_pv_page_urlhost
    , i.page_urlpath as i_pv_page_urlpath
    , ROW_NUMBER() OVER ( PARTITION BY c.event_id ORDER BY i.collector_tstamp DESC) AS idx
  FROM {{ ref('stg_events__optins_orders')}} c
  LEFT JOIN {{ ref('stg_events__affiliate_hits')}} i
    ON c.user_ipaddress = i.user_ipaddress
    AND c.collector_tstamp > i.collector_tstamp
)

SELECT *
FROM ip_attribution_rank