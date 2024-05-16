{{ config(
    materialized = 'table'
)}}


with orders as (
    SELECT 
        * except(final_affid_attribution)
        , DATETIME(collector_tstamp, 'US/Arizona') as collector_tstamp_az
        , DATE(collector_tstamp, 'US/Arizona') as date_az
        , IF(
              IFNULL(REGEXP_EXTRACT(final_affid_attribution, r'^(.*?)\?'), final_affid_attribution)
              = '100000','11585',IFNULL(REGEXP_EXTRACT(final_affid_attribution, r'^(.*?)\?'), final_affid_attribution)
              )
               as final_affid_attribution          
    FROM {{ ref('int_attribution__final')}} 
    WHERE se_action = 'order'
)

, affiliate_id_numeric as (
  select *
    , case when final_affid_attribution is null then 'already_null' 
        when SAFE_CAST(final_affid_attribution AS INT64) is null then 'error'
        else 'numeric' end as numeric_test
  from orders
  
)


select *
from affiliate_id_numeric
where numeric_test <> 'error'
    -- and lower(user_id) not in (
    --     'kylecmalone@gmail.com' , 'kylemguy@gmail.com' , 'kylemalone321@gmail.com'
    -- )
order by collector_tstamp

