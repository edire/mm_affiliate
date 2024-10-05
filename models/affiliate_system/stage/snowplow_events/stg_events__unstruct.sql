
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

, affiliate_hits as (
  SELECT 
    '{{ env_var("OFFER") }}' as app_id
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
  WHERE DATE(collector_tstamp, 'US/Arizona') >= '{{ env_var("START_DATE") }}'
    and event = 'unstruct'
    and (page_urlhost in (select url_host from url_host_tbl)
      or lower(app_id) in ('{{ env_var("OFFER") }}'))
    -- AND unstruct_event_com_deangraziosi_affiliate_tracking_2_1_0_2.affiliate_id IS NOT NULL
    -- AND unstruct_event_com_deangraziosi_affiliate_tracking_2_1_0_2.affiliate_id <> '0'
    and event_name = 'affiliate_tracking_2'
)

SELECT *
FROM affiliate_hits