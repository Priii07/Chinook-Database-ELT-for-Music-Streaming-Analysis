with f_revenue as (
    SELECT * from {{ ref('fact_revenue') }}
),
d_date as (
    SELECT * from {{ ref('dim_date') }}
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
    d.*,
    f.invoicedatekey,
    f.quantity,
    f.totalearnings,
    f.billingaddress,
    f.billingcity,
    f.billingstate,
    f.billingcountry,
    f.billingpostalcode
FROM f_revenue f 
left join d_customer c on f.customerkey = c.customerkey
left join d_track t on f.trackkey = t.trackkey
left join d_date d on f.invoicedatekey = d.datekey