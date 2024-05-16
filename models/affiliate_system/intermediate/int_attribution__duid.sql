
{{ config(
    materialized = 'table'
)}}


WITH domain_attribution_rank as (
  SELECT 
    c.*
    , d.domain_userid AS d_pv_domain_userid	
    , d.affiliate_id AS d_pv_affiliate_id	
    , d.sub_id AS d_pv_sub_id	
    , d.collector_tstamp AS d_pv_collector_tstamp	
    , d.event_id AS d_pv_event_id
    , d.page_urlhost as d_pv_page_urlhost
    , d.page_urlpath as d_pv_page_urlpath
    , ROW_NUMBER() OVER ( PARTITION BY c.event_id ORDER BY d.collector_tstamp DESC) AS idx

  FROM {{ ref('stg_events__optins_orders')}}  c
  LEFT JOIN {{ ref('stg_events__affiliate_hits')}} d
    ON c.domain_userid = d.domain_userid
    AND c.collector_tstamp > d.collector_tstamp
)


SELECT *
FROM domain_attribution_rank
