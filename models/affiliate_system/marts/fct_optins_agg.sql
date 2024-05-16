with optins as (
  SELECT 
    date_az
    , app_id
    , se_action
    , lower(user_id) as email
    , final_affid_attribution
    , final_subid_attribution
  FROM {{ref('fct_optins')}}
)

, optins_agg as (
  select 
    date_az
    , app_id
    , final_affid_attribution as affiliate_id
    , final_subid_attribution as sub_id
    , count(distinct email) as optins
  from optins
    group by date_az
    , app_id
    , final_affid_attribution
    , final_subid_attribution
)


select *
from optins_agg
where date_az >= '2024-04-01'
order by 1 desc 

