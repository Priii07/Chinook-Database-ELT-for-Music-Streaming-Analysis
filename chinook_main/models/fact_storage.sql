with stg_invoice as (
    SELECT * FROM {{ source('chinook', 'Invoice') }}
),
stg_invoiceline as (
    SELECT 
        invoiceid, 
        trackid,
        sum(quantity) as quantity,
        sum(quantity*unitprice) as totalearnings
    FROM {{ source('chinook', 'InvoiceLine') }}
    GROUP BY invoiceid, trackid
),
stg_tracks as (
    SELECT * FROM {{ source('chinook', 'Track') }}
)

SELECT
    il.invoiceid, 
    {{ dbt_utils.generate_surrogate_key(['t.trackid'])}} as trackkey,
    replace(to_date(i.invoicedate)::varchar,'-','')::int as invoicedatekey,
    il.quantity,
    il.totalearnings,
    ROUND((t.milliseconds)/(1000 * 60),2) as tracklengthinmins,
    ROUND((t.bytes / 1024),2) as tracksizeinmb,
    i.billingaddress,
    i.billingcity,
    i.billingstate,
    i.billingcountry,
    i.BILLINGPOSTALCODE
FROM stg_invoiceline il
JOIN stg_invoice i on il.invoiceid = i.invoiceid 
JOIN stg_tracks t on il.trackid = t.trackid