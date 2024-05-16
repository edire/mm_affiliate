
WITH domain_attr as (
    SELECT *
    FROM {{ ref('int_attribution__duid') }}

)

, ip_attr as (
    SELECT *
    FROM {{ ref('int_attribution__ip') }} 

)

, attribution_join as (
    SELECT 
    d.*
    , i_pv_user_ipaddress	
    , i_pv_affiliate_id	
    , i_pv_sub_id	
    , i_pv_collector_tstamp	
    , i_pv_event_id
    , i_pv_page_urlhost
    , i_pv_page_urlpath
    , coalesce(d_pv_affiliate_id,i_pv_affiliate_id) as int_attrib_affiliate_id
    , coalesce(d_pv_sub_id,i_pv_sub_id) as int_attrib_sub_id

    , CASE WHEN (d_pv_collector_tstamp is null AND i_pv_collector_tstamp is not null) then i_pv_affiliate_id
            WHEN (d_pv_collector_tstamp is not null AND i_pv_collector_tstamp is null) then d_pv_affiliate_id
            WHEN (d_pv_collector_tstamp is null AND i_pv_collector_tstamp is null) then null
            WHEN d_pv_collector_tstamp >= i_pv_collector_tstamp then d_pv_affiliate_id
            WHEN d_pv_collector_tstamp < i_pv_collector_tstamp then i_pv_affiliate_id
            ELSE 'other'
            END as new_affid_attribution

    , CASE WHEN (d_pv_collector_tstamp is null AND i_pv_collector_tstamp is not null) then i_pv_sub_id
            WHEN (d_pv_collector_tstamp is not null AND i_pv_collector_tstamp is null) then d_pv_sub_id
            WHEN (d_pv_collector_tstamp is null AND i_pv_collector_tstamp is null) then null
            WHEN d_pv_collector_tstamp >= i_pv_collector_tstamp then d_pv_sub_id
            WHEN d_pv_collector_tstamp < i_pv_collector_tstamp then i_pv_sub_id
            ELSE 'other'
            END as new_subid_attribution

    , CASE WHEN (d_pv_collector_tstamp is null AND i_pv_collector_tstamp is not null) then i_pv_collector_tstamp
            WHEN (d_pv_collector_tstamp is not null AND i_pv_collector_tstamp is null) then d_pv_collector_tstamp
            WHEN (d_pv_collector_tstamp is null AND i_pv_collector_tstamp is null) then null
            WHEN d_pv_collector_tstamp >= i_pv_collector_tstamp then d_pv_collector_tstamp
            WHEN d_pv_collector_tstamp < i_pv_collector_tstamp then i_pv_collector_tstamp
            ELSE null
            END as new_affid_tstamp_used

    , CASE WHEN (d_pv_collector_tstamp is null AND i_pv_collector_tstamp is not null) then 'IP'
            WHEN (d_pv_collector_tstamp is not null AND i_pv_collector_tstamp is null) then 'domain_userid'
            WHEN (d_pv_collector_tstamp is null AND i_pv_collector_tstamp is null) then null
            WHEN d_pv_collector_tstamp >= i_pv_collector_tstamp then 'domain_userid'
            WHEN d_pv_collector_tstamp < i_pv_collector_tstamp then 'IP'
            ELSE 'other'
            END as attribution_source

        , CASE WHEN (d_pv_collector_tstamp is null AND i_pv_collector_tstamp is not null) then i_pv_page_urlpath
            WHEN (d_pv_collector_tstamp is not null AND i_pv_collector_tstamp is null) then d_pv_page_urlpath
            WHEN (d_pv_collector_tstamp is null AND i_pv_collector_tstamp is null) then null
            WHEN d_pv_collector_tstamp >= i_pv_collector_tstamp then d_pv_page_urlpath
            WHEN d_pv_collector_tstamp < i_pv_collector_tstamp then i_pv_page_urlpath
            ELSE 'other'
            END as attribution_url_path

    FROM domain_attr d
    LEFT JOIN ip_attr i
        on d.event_id = i.event_id
        and i.idx = 1
    WHERE d.idx = 1

)


SELECT *
FROM attribution_join

