SELECT 
    datekey::int as datekey,
    date,
    year, 
    month, 
    quarter,
    day,
    dayofweek,
    weekofyear,
    dayofyear,
    quartername,
    monthname,
    dayname,
    weekday
FROM {{ source('conformed', 'DateDimension') }}