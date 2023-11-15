with f_storage as (
    SELECT * from {{ ref('fact_storage') }}
),
d_tracks as (
    SELECT * FROM {{ ref('dim_tracks') }}
),
d_date as (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT  
    t.*, 
    d.*,
    f.invoicedatekey,
    f.quantity, 
    f.totalearnings,
    f.tracklengthinmins,
    f.tracksizeinmb
FROM f_storage f 
LEFT JOIN d_tracks t on f.trackkey = t.trackkey 
LEFT JOIN d_date d on f.invoicedatekey = d.datekey