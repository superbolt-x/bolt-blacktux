{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue,
act.complete_registration
FROM {{ ref('facebook_performance_by_ad') }}
LEFT JOIN 
    (SELECT DATE_TRUNC('day',date) as date, 'day' as date_granularity, ad_id,
        COALESCE(SUM("7_d_click"),0) as complete_registration
    FROM {{ source('facebook_raw','ads_insights_actions') }}
    WHERE action_type = 'complete_registration'
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('week',date) as date, 'week' as date_granularity, ad_id,
        COALESCE(SUM("7_d_click"),0) as complete_registration
    FROM {{ source('facebook_raw','ads_insights_actions') }}
    WHERE action_type = 'complete_registration'
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('month',date) as date, 'month' as date_granularity, ad_id,
        COALESCE(SUM("7_d_click"),0) as complete_registration
    FROM {{ source('facebook_raw','ads_insights_actions') }}
    WHERE action_type = 'complete_registration'
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('quarter',date) as date, 'quarter' as date_granularity, ad_id,
        COALESCE(SUM("7_d_click"),0) as complete_registration
    FROM {{ source('facebook_raw','ads_insights_actions') }}
    WHERE action_type = 'complete_registration'
    GROUP BY 1,2,3
    UNION ALL
    SELECT DATE_TRUNC('year',date) as date, 'year' as date_granularity, ad_id,
        COALESCE(SUM("7_d_click"),0) as complete_registration
    FROM {{ source('facebook_raw','ads_insights_actions') }}
    WHERE action_type = 'complete_registration'
    GROUP BY 1,2,3) act
USING(ad_id,date,date_granularity)
