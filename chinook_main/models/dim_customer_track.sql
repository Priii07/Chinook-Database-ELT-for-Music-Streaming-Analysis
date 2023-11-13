with stg_customer_track as (
    SELECT * FROM {{ source('chinook', 'CustomerTrackReviw') }}
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
    concat(c.firstname , ' ' , c.lastname) as customername,
    t.name as trackname,
    ct.sentiment
FROM stg_customer_track ct
JOIN stg_customers c on ct.customerid = c.customerid
JOIN stg_tracks t on t.trackid = ct.trackid