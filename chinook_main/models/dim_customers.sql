with stg_customers as (
    SELECT * FROM {{ source('chinook', 'Customers') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_customers.customerid'])}} as customerkey,
    customerid, 
    concat(lastname , ', ' , firstname) as customernamelastfirst,
    concat(firstname , ' ' , lastname) as customernamefirstlast,
    company as customercompany,
    address as customeraddress,
    city as customercity,
    state as customerstate, 
    postalcode as customerpostalcode,
    country as customercountry, 
    phone as customerphone,
    fax as customerfax,
    email as customeremail,
    supportrepid as customersupportrepid
FROM stg_customers 
