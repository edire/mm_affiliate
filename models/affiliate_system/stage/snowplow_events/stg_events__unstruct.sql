
{{ config(
    materialized = 'table'
)}}

WITH affiliate_hits as (
  SELECT 
    'mbs' as app_id
    , domain_userid 
    , user_ipaddress 
    , collector_tstamp
    , event_id
    , page_urlhost
    , page_urlpath
    , unstruct_event_com_deangraziosi_affiliate_tracking_2_1_0_2.affiliate_id as affiliate_id
    , unstruct_event_com_deangraziosi_affiliate_tracking_2_1_0_2.sub_id as sub_id
    , geo_country	
    , geo_region	
    , geo_city
  FROM {{ source('raw_events', 'events') }} 
  WHERE DATE(collector_tstamp, 'US/Arizona') >= '2024-04-01'
    and event = 'unstruct'
    and (page_urlhost in ('gamehaschangedevent.com')
      or lower(app_id) in ('mbs'))
    -- AND unstruct_event_com_deangraziosi_affiliate_tracking_2_1_0_2.affiliate_id IS NOT NULL
    -- AND unstruct_event_com_deangraziosi_affiliate_tracking_2_1_0_2.affiliate_id <> '0'
    and event_name = 'affiliate_tracking_2'
)

SELECT *
FROM affiliate_hits