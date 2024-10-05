
{{ config(
    materialized = 'table'
)}}

WITH url_host_env AS (
  SELECT '{{ env_var("URL_HOST") }}' AS url_host_str
)

, url_host_tbl as (
  SELECT TRIM(url_host_val) AS url_host
  FROM url_host_env,
    UNNEST(SPLIT(url_host_str, ',')) AS url_host_val
)

, conversions AS (
  SELECT 
    '{{ env_var("OFFER") }}' as app_id
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
  WHERE DATE(collector_tstamp, 'US/Arizona') >= '{{ env_var("START_DATE") }}'
    AND se_action IN ('optin','order')
    and (page_urlhost in (select url_host from url_host_tbl)
      or lower(app_id) in ('{{ env_var("OFFER") }}'))
)

SELECT *
FROM conversions