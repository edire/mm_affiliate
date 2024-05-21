
with optins as (
  SELECT date_az
    , app_id
    , se_action
    , lower(user_id) as email
    , final_affid_attribution
    , final_subid_attribution
    , is_new_customer
    , is_mmcon_customer
    , is_new_since_ttt
  FROM {{ref('fct_optins')}}
  QUALIFY ROW_NUMBER() OVER (partition by lower(user_id) order by date_az) = 1
)

, optins_agg as (
  select date_az
    , app_id
    , final_affid_attribution as affiliate_id
    , final_subid_attribution as sub_id
    , count(distinct email) as optins
    , is_new_customer
    , is_mmcon_customer
    , is_new_since_ttt
  from optins
  group by date_az
    , app_id
    , final_affid_attribution
    , final_subid_attribution
    , is_new_customer
    , is_mmcon_customer
    , is_new_since_ttt
)

select *
from optins_agg