with stg_customers as (
    SELECT * FROM {{ source('chinook', 'Customers') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_customers.customerid'])}} as customerkey,
    customerid, 
    concat(lastname , ', ' , firstname) as customernamelastfirst,
    concat(firstname , ' ' , lastname) as customernamefirstlast,
    CASE
        WHEN company is NULL THEN 'no company'
        ELSE company
    END as customercompany,
    address as customeraddress,
    city as customercity,
    CASE 
        WHEN state is NULL THEN 'no state'
        ELSE state
    END as customerstate, 
    CASE
        WHEN postalcode is null THEN 'no postalcode'
        ELSE postalcode
    END as customerpostalcode,
    country as customercountry, 
    CASE
        WHEN phone is null THEN 'no phone number'
        ELSE phone
    END as customerphone,
    CASE
        WHEN fax is null THEN 'no fax'
        ELSE fax
    END as customerfax,
    email as customeremail,
    supportrepid as customersupportrepid
FROM stg_customers 
