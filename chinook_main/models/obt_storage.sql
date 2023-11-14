with f_storage as (
    SELECT * from {{ ref('fact_storage') }}
),
d_tracks as (
    SELECT * FROM {{ ref('dim_tracks') }}
)

SELECT  
    t.*, 
    f.invoicedatekey,
    f.quantity, 
    f.totalearnings,
    f.tracklengthinmins,
    f.tracksizeinmb,
    f.billingaddress,
    f.billingcity,
    f.billingstate,
    f.billingcountry,
    f.BILLINGPOSTALCODE
FROM f_storage f 
LEFT JOIN d_tracks t on f.trackkey = t.trackkey 