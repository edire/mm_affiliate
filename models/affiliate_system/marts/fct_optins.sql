
{{ config(
    materialized = 'table'
)}}

with optins as (
    SELECT 
        * except(final_affid_attribution)
        , DATETIME(collector_tstamp, 'US/Arizona') as collector_tstamp_az
        , DATE(collector_tstamp, 'US/Arizona') as date_az
        , IFNULL(REGEXP_EXTRACT(final_affid_attribution, r'^(.*?)\?'), final_affid_attribution) as final_affid_attribution
    FROM {{ ref('int_attribution__final')}} 
    WHERE se_action = 'optin'
)

, affiliate_id_numeric as (
  select *
    , case when final_affid_attribution is null then 'already_null' 
        when SAFE_CAST(final_affid_attribution AS INT64) is null then 'error'
        else 'numeric' end as numeric_test
  from optins
)

, join_contact_info as (
  select 
    a.*
    , c.dt_captured
    , c.funnel_id_captured
    , case when c.funnel_id_captured = '12962498' then 1 else 0 end as is_new_customer
    , case when c.funnel_id_captured = '12799672' then 1 else 0 end as is_mmcon_customer
    , case when date(c.dt_captured) >= '2022-09-01' then 1 else 0 end as is_new_since_ttt
  from affiliate_id_numeric a
  left join bbg-platform.analytics.dim_contacts c
    on lower(a.user_id) = lower(c.email)
  where numeric_test <> 'error'
)

select *
from join_contact_info