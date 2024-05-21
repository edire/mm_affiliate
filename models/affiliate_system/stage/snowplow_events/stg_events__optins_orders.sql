
{{ config(
    materialized = 'table'
)}}

WITH conversions AS (
  SELECT 
    'mbs' as app_id
    , collector_tstamp
    , event
    , event_id
    , user_id
    , user_ipaddress
    , domain_userid
    , geo_country	
    , geo_region	
    , geo_city	
    , geo_zipcode
    , geo_region_name
    , se_action
    , tr_orderid
    , ti_orderid	
    , ti_sku	
    , ti_name
    , ti_price
    , tr_total
    , unstruct_event_com_deangraziosi_affiliate_tracking_2_1_0_2.affiliate_id as affiliate_id
    , unstruct_event_com_deangraziosi_affiliate_tracking_2_1_0_2.sub_id as sub_id
    , unstruct_event_com_deangraziosi_clickfunnels_tracking_2_0_2.funnel_step_id as funnel_step_id
    , case when unstruct_event_com_deangraziosi_clickfunnels_tracking_2_0_2.funnel_step_id = 87279813 then 1 else 0 end as is_paypal_optin
  FROM {{ source('raw_events', 'events') }} 
  WHERE DATE(collector_tstamp, 'US/Arizona') >= '2024-04-01'
    AND se_action IN ('optin','order')
    and (page_urlhost in ('gamehaschangedevent.com')
      or lower(app_id) in ('mbs'))
)

SELECT *
FROM conversions