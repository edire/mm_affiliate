
with optins as (
  SELECT 
    date_az
    , app_id
    , se_action
    , lower(user_id) as email
    , final_affid_attribution
    , final_subid_attribution
    , geo_country
    , geo_region
    , is_new_customer
    , is_mmcon_customer
    , is_new_since_ttt
--   FROM `snowplow-348319`.`dbt_kmalone`.`oyf_fct_optins`
  from {{ref('fct_optins')}}
  QUALIFY ROW_NUMBER() OVER (partition by lower(user_id) order by date_az) = 1
)

, countries as (
  select *
  from snowplow-348319.oyf2023.ttt_pn_conversions_country 
)

, join_countries as (
  select 
    o.*
    , c.country_name
    , coalesce(c.is_tier1_top6,0) as is_tier1_top6
    , coalesce(c.is_expanded_tier1,0) as is_expanded_tier1
    , c.pn_conversion_rate
    , coalesce(c.pn_conversion_rate,0.00822) as expected_conv_rate
  from optins o
  left join countries c
    on o.geo_country = c.geo_country
)

-- select *
-- from join_countries


, optins_agg as (
  select 
    date_az
    , app_id
    , country_name
    , is_tier1_top6
    , is_expanded_tier1
    , expected_conv_rate
    , is_new_customer
    , is_mmcon_customer
    , is_new_since_ttt
    , count(email) as optins
    , round(count(email)*expected_conv_rate,6) as expected_lp_sales
  from join_countries
    group by 1,2,3,4,5,6,7,8,9
)


select 
  *
from optins_agg
where date_az >= '2024-04-01'
order by 1 desc 
