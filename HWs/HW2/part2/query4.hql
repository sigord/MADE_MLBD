WITH subquery AS (
    SELECT trim(tag) AS tag, artist_lastfm, listeners_lastfm, mdid
    FROM hw2.artist AS a
    LATERAL VIEW explode(tags_lastfm) a as tag
)
SELECT t3.country_mb, count(*) AS country_count FROM
(SELECT t2.mdid, a.artist_lastfm, a.tags_lastfm, a.country_mb FROM
(SELECT t1.mdid, subquery.tag, subquery.artist_lastfm FROM
(SELECT DISTINCT mdid FROM subquery WHERE tag == 'composer') t1
JOIN subquery ON t1.mdid = subquery.mdid
WHERE tag == 'Classical') t2
JOIN hw2.artist a ON a.mdid = t2.mdid) t3
GROUP BY t3.country_mb
ORDER BY country_count DESC;