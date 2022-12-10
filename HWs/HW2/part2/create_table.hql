CREATE DATABASE IF NOT EXISTS HW2;

CREATE TABLE IF NOT EXISTS HW2.artist (
 mdid string,
 artist_mb string,
 artist_lastfm string,
 country_mb string,
 country_lastfm array<string>,
 tags_mb array<string>,
 tags_lastfm array<string>,
 listeners_lastfm int,
 scrobbles_lastfm int,
 ambigous_artist boolean)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ";"
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/opt/artists.csv' INTO TABLE HW2.artist;