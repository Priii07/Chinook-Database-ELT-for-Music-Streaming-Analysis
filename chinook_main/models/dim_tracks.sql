with stg_tracks as (
    SELECT * FROM {{ source('chinook', 'Track') }}
),
stg_album as (
    SELECT * FROM {{ source('chinook', 'Album') }}
),
stg_artist as (
    SELECT * FROM {{ source('chinook', 'Artist') }}
),
stg_mediatype as (
    SELECT * FROM {{ source('chinook', 'MediaType') }}
),
stg_genre as (
    SELECT * FROM {{ source('chinook', 'Genre') }}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['t.trackid'])}} as trackkey,
    t.trackid,
    t.name as trackname,
    al.title as albumtitle,
    a.name as artistname,
    m.name as mediatypename,
    g.name as genrename,
    t.composer as composername
FROM stg_tracks t 
LEFT JOIN stg_album al on t.albumid = al.albumid
LEFT JOIN stg_artist a on al.artistid = a.artistid
LEFT JOIN stg_mediatype m on t.mediatypeid = m.mediatypeid
LEFT JOIN stg_genre g on t.genreid = g.genreid


