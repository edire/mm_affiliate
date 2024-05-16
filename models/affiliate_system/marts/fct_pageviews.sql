{{ config(
    materialized = 'table'
)}}


with page_views as (
    select 
        date(collector_tstamp, 'US/Arizona') as date_az
        , REGEXP_REPLACE(CAST(IFNULL(REGEXP_EXTRACT(affiliate_id, r'^(.*?)\?'), affiliate_id) AS STRING), r'[^a-zA-Z0-9]', '') as affiliate_id
        , REGEXP_REPLACE(CAST(sub_id AS STRING), r'[^a-zA-Z0-9]', '') as sub_id
        , concat(page_urlhost,page_urlpath) as url_path
        , count(*) as total_page_views
        , count(distinct user_ipaddress) as unique_page_views
        , app_id

    FROM {{ ref('stg_events__affiliate_hits') }}

    WHERE page_urlhost in ('gamehaschangedevent.com')
    -- and (page_urlpath like ('%join-now%')
    --     or page_urlpath like ('%save-my-seat%')
    -- )

    GROUP BY date_az
        , affiliate_id
        , sub_id
        , url_path
        , app_id
)

, add_pk as (
    select 
    left(concat(date_az,affiliate_id,sub_id,url_path),150) as id
    , *
    from page_views
)


, affiliate_id_numeric as (
    select *
    , case when affiliate_id is null then 'already_null' 
        when SAFE_CAST(affiliate_id AS INT64) is null then 'error'
        else 'numeric' end as numeric_test
    from add_pk
)


select *
from affiliate_id_numeric
where numeric_test <> 'error'
order by date_az desc
