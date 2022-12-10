SELECT trim(tag) AS tag, count(1) AS tag_count
FROM HW2.artist AS a
LATERAL VIEW explode(tags_lastfm) a AS tag
GROUP BY trim(tag)
ORDER BY tag_count DESC
LIMIT 1;