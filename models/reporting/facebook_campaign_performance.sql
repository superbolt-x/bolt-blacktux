{{ config (
    alias = target.database + '_facebook_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
case 
    when campaign_id = '6633504449611' and date_granularity = 'day' and date between '2024-12-27' and '2025-01-08' then 'Pre-Segmentation'
    when campaign_id = '6633504449611' and date_granularity = 'day' and date between '2025-01-08' and '2025-02-03' then 'Post-Segmentation'
else 'Other' end as period,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases_7_d_click as purchases,
purchases as "purchases_7dc_1dv",
revenue_7_d_click as revenue,
revenue as "revenue_7dc_1dv",
ins."offsite_conversion.fb_pixel_custom.shopifycompleteweddingpartyleadform" as "shopify_lead_7dc_1dv",
conv."shopify_lead",
ins."offsite_conversion.fb_pixel_custom.shopifycreatorcheckout" as "shopify_checkout_7dc_1dv",
ins."offsite_conversion.fb_pixel_custom.shopifycreatorcheckout_value" as "shopify_checkout_value_7dc_1dv",
conv."shopify_checkout",
conv_val."shopify_checkout_value",
ins.complete_registration as "complete_registration_7dc_1dv",
act.complete_registration,
ins."offsite_conversion.fb_pixel_custom.appointmentformsubmit" as "appointment_form_submit_7dc_1dv",
conv.appointment_form_submit
FROM {{ ref('facebook_performance_by_campaign') }} ins
LEFT JOIN 
    (SELECT DATE_TRUNC('day',date) as date, 'day' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type = 'complete_registration' THEN "_7_d_click" ELSE 0 END),0) as complete_registration
    FROM {{ source('facebook_raw','campaigns_insights_actions') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('week',date) as date, 'week' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type = 'complete_registration' THEN "_7_d_click" ELSE 0 END),0) as complete_registration
    FROM {{ source('facebook_raw','campaigns_insights_actions') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('month',date) as date, 'month' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type = 'complete_registration' THEN "_7_d_click" ELSE 0 END),0) as complete_registration
    FROM {{ source('facebook_raw','campaigns_insights_actions') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('quarter',date) as date, 'quarter' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type = 'complete_registration' THEN "_7_d_click" ELSE 0 END),0) as complete_registration
    FROM {{ source('facebook_raw','campaigns_insights_actions') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('year',date) as date, 'year' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type = 'complete_registration' THEN "_7_d_click" ELSE 0 END),0) as complete_registration
    FROM {{ source('facebook_raw','campaigns_insights_actions') }}
    GROUP BY 1,2,3) act
USING(campaign_id,date,date_granularity)
LEFT JOIN 
    (SELECT DATE_TRUNC('day',date) as date, 'day' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycompleteweddingpartyleadform' THEN "_7_d_click" ELSE 0 END),0) as shopify_lead,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycreatorcheckout' THEN "_7_d_click" ELSE 0 END),0) as shopify_checkout,
        COALESCE(SUM(CASE WHEN action_type ~* 'appointmentformsubmit' THEN "_7_d_click" ELSE 0 END),0) as appointment_form_submit
    FROM {{ source('facebook_raw','campaigns_insights_conversions') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('week',date) as date, 'week' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycompleteweddingpartyleadform' THEN "_7_d_click" ELSE 0 END),0) as shopify_lead,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycreatorcheckout' THEN "_7_d_click" ELSE 0 END),0) as shopify_checkout,
        COALESCE(SUM(CASE WHEN action_type ~* 'appointmentformsubmit' THEN "_7_d_click" ELSE 0 END),0) as appointment_form_submit
    FROM {{ source('facebook_raw','campaigns_insights_conversions') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('month',date) as date, 'month' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycompleteweddingpartyleadform' THEN "_7_d_click" ELSE 0 END),0) as shopify_lead,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycreatorcheckout' THEN "_7_d_click" ELSE 0 END),0) as shopify_checkout,
        COALESCE(SUM(CASE WHEN action_type ~* 'appointmentformsubmit' THEN "_7_d_click" ELSE 0 END),0) as appointment_form_submit
    FROM {{ source('facebook_raw','campaigns_insights_conversions') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('quarter',date) as date, 'quarter' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycompleteweddingpartyleadform' THEN "_7_d_click" ELSE 0 END),0) as shopify_lead,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycreatorcheckout' THEN "_7_d_click" ELSE 0 END),0) as shopify_checkout,
        COALESCE(SUM(CASE WHEN action_type ~* 'appointmentformsubmit' THEN "_7_d_click" ELSE 0 END),0) as appointment_form_submit
    FROM {{ source('facebook_raw','campaigns_insights_conversions') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('year',date) as date, 'year' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycompleteweddingpartyleadform' THEN "_7_d_click" ELSE 0 END),0) as shopify_lead,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycreatorcheckout' THEN "_7_d_click" ELSE 0 END),0) as shopify_checkout,
        COALESCE(SUM(CASE WHEN action_type ~* 'appointmentformsubmit' THEN "_7_d_click" ELSE 0 END),0) as appointment_form_submit
    FROM {{ source('facebook_raw','campaigns_insights_conversions') }}
    GROUP BY 1,2,3) conv
USING(campaign_id,date,date_granularity)
LEFT JOIN 
    (SELECT DATE_TRUNC('day',date) as date, 'day' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycreatorcheckout' THEN "_7_d_click" ELSE 0 END),0) as shopify_checkout_value
    FROM {{ source('facebook_raw','campaigns_insights_conversion_values') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('week',date) as date, 'week' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycreatorcheckout' THEN "_7_d_click" ELSE 0 END),0) as shopify_checkout_value
    FROM {{ source('facebook_raw','campaigns_insights_conversion_values') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('month',date) as date, 'month' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycreatorcheckout' THEN "_7_d_click" ELSE 0 END),0) as shopify_checkout_value
    FROM {{ source('facebook_raw','campaigns_insights_conversion_values') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('quarter',date) as date, 'quarter' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycreatorcheckout' THEN "_7_d_click" ELSE 0 END),0) as shopify_checkout_value
    FROM {{ source('facebook_raw','campaigns_insights_conversion_values') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('year',date) as date, 'year' as date_granularity, campaign_id,
        COALESCE(SUM(CASE WHEN action_type ~* 'shopifycreatorcheckout' THEN "_7_d_click" ELSE 0 END),0) as shopify_checkout_value
    FROM {{ source('facebook_raw','campaigns_insights_conversion_values') }}
    GROUP BY 1,2,3) conv_val
USING(campaign_id,date,date_granularity)
