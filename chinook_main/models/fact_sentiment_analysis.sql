with stg_customer_track as (
    SELECT 
        customerid,
        trackid,
        sentiment,
        count(sentiment) as sentiment_count
    FROM {{ source('chinook', 'CustomerTrackReviw') }}
    GROUP BY customerid, trackid, sentiment
), 
stg_customers as (
    SELECT * FROM {{ source('chinook', 'Customers') }}
),
stg_tracks as (
    SELECT * FROM {{ source('chinook', 'Track') }}
)

SELECT 
        {{ dbt_utils.generate_surrogate_key(['ct.customerid'])}} as customerkey,
        {{ dbt_utils.generate_surrogate_key(['ct.trackid'])}} as trackkey,
        ct.sentiment_count,
        CASE 
            WHEN ct.sentiment = 'like' THEN 1
            ELSE 0
        END as like_sentiment,
        CASE 
            WHEN ct.sentiment = 'dislike' THEN '1'
            Else 0
        END as sentiment_dislike
FROM stg_customer_track ct
LEFT JOIN stg_customers c on ct.customerid = c.customerid
LEFT JOIN stg_tracks t on t.trackid = ct.trackid