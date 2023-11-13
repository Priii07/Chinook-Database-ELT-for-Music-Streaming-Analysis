with f_sentiment_analysis as (
    SELECT * FROM {{ ref('fact_sentiment_analysis') }}
),
d_customer as (
    SELECT * FROM {{ ref('dim_customers') }}
),
d_track as (
    SELECT * FROM {{ ref('dim_tracks') }}
)
SELECT 
    c.*,
    t.*,
    f.sentiment_count,
    f.like_sentiment,
    f.sentiment_dislike
FROM f_sentiment_analysis f 
left join d_customer c on f.customerkey = c.customerkey
left join d_track t on f.trackkey = t.trackkey
