

{{ config(
    materialized = 'table'
)}}


with final_attribution as (
select 
  *
  , case when e_pv_event_id = event_id then attribution_source
        when new_affid_attribution is null and e_pv_affiliate_id is not null then 'email_affid'
        when new_affid_attribution is not null and e_pv_affiliate_id is not null 
              and e_pv_collector_tstamp > new_affid_tstamp_used then 'email_affid_override'
        when new_affid_attribution is not null and e_pv_affiliate_id is not null 
              and e_pv_collector_tstamp <= new_affid_tstamp_used then attribution_source
        else null end as final_attribution_source
  
  , case when e_pv_event_id = event_id then new_affid_attribution
        when new_affid_attribution is null and e_pv_affiliate_id is not null then e_pv_affiliate_id
        when new_affid_attribution is not null and e_pv_affiliate_id is not null 
              and e_pv_collector_tstamp > new_affid_tstamp_used then e_pv_affiliate_id
        when new_affid_attribution is not null and e_pv_affiliate_id is not null 
              and e_pv_collector_tstamp <= new_affid_tstamp_used then new_affid_attribution
        else null end as final_affid_attribution

  , case when e_pv_event_id = event_id then new_subid_attribution
        when new_affid_attribution is null and e_pv_affiliate_id is not null then e_pv_sub_id
        when new_affid_attribution is not null and e_pv_affiliate_id is not null 
              and e_pv_collector_tstamp > new_affid_tstamp_used then e_pv_sub_id
        when new_affid_attribution is not null and e_pv_affiliate_id is not null 
              and e_pv_collector_tstamp <= new_affid_tstamp_used then new_subid_attribution
        else null end as final_subid_attribution

  , case when e_pv_event_id = event_id then new_affid_tstamp_used
        when new_affid_attribution is null and e_pv_affiliate_id is not null then e_pv_collector_tstamp
        when new_affid_attribution is not null and e_pv_affiliate_id is not null 
              and e_pv_collector_tstamp > new_affid_tstamp_used then e_pv_collector_tstamp
        when new_affid_attribution is not null and e_pv_affiliate_id is not null 
              and e_pv_collector_tstamp <= new_affid_tstamp_used then new_affid_tstamp_used
        else null end as final_attribution_tstamp

from {{ ref('int_attribution__add_email') }}
WHERE email_idx = 1
)


, cast_to_0 as (
      select 
            app_id
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
            , affiliate_id
            , sub_id
            , d_pv_domain_userid
            , d_pv_affiliate_id
            , d_pv_sub_id
            , d_pv_collector_tstamp
            , d_pv_event_id
            , d_pv_page_urlhost
            , d_pv_page_urlpath
            , idx
            , i_pv_user_ipaddress
            , i_pv_affiliate_id
            , i_pv_sub_id
            , i_pv_collector_tstamp
            , i_pv_event_id
            , i_pv_page_urlhost
            , i_pv_page_urlpath
            , int_attrib_affiliate_id
            , int_attrib_sub_id
            , new_affid_attribution
            , new_subid_attribution
            , new_affid_tstamp_used
            , attribution_source
            , attribution_url_path
            , e_user_id
            , e_pv_affiliate_id
            , e_pv_sub_id
            , e_pv_collector_tstamp
            , e_pv_event_id
            , email_idx
            , final_attribution_source
            , case 
                  when (final_affid_attribution is null 
                  or final_affid_attribution = '')
                  then '0' else final_affid_attribution end as final_affid_attribution
            , case 
                  when (final_subid_attribution is null 
                  or final_subid_attribution = '')
                  then '0' else final_subid_attribution end as final_subid_attribution
            , final_attribution_tstamp
            , funnel_step_id
            , is_paypal_optin

      from final_attribution
)




select *
from cast_to_0
order by collector_tstamp desc


