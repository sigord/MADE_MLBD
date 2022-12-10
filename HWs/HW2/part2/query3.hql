WITH subquery as (
    SELECT trim(tag) AS tag, artist_lastfm, listeners_lastfm FROM hw2.artist AS a
    LATERAL VIEW explode(tags_lastfm) a as tag
)

SELECT subquery.*, rank() OVER (PARTITION BY tag ORDER BY listeners_lastfm DESC) AS rank
FROM subquery
WHERE tag IN (
    SELECT tag FROM (
        SELECT tag, count(*) AS tag_count FROM subquery
        GROUP BY tag
        ORDER BY  tag_count DESC
        LIMIT 10
    ) AS t2
)
ORDER BY rank ASC , listeners_lastfm DESC
LIMIT 10;