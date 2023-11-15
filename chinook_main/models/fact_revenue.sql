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
stg_customers as (
    SELECT * FROM {{ source('chinook', 'Customers') }}
),
stg_tracks as (
    SELECT * FROM {{ source('chinook', 'Track') }}
)
SELECT
    il.invoiceid, 
    {{ dbt_utils.generate_surrogate_key(['c.customerid'])}} as customerkey,
    {{ dbt_utils.generate_surrogate_key(['t.trackid'])}} as trackkey,
    replace(to_date(i.invoicedate)::varchar,'-','')::int as invoicedatekey,
    il.quantity,
    il.totalearnings,
    i.billingaddress,
    i.billingcity,
    i.billingstate,
    i.billingcountry,
    i.billingpostalcode
FROM stg_invoice i 
JOIN stg_invoiceline il on i.invoiceid = il.invoiceid 
JOIN stg_customers c on i.customerid = c.customerid
join stg_tracks t on il.trackid = t.trackid