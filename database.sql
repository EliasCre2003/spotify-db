CREATE TABLE songs (
    "url" VARCHAR(255) PRIMARY KEY,
    song_name VARCHAR(255),
    artist_name VARCHAR(255),
    album_name VARCHAR(255)
);

CREATE TABLE song_streams (
    stream_id SERIAL PRIMARY KEY,
    time_stamp DATETIME,
    ms_listened INT,
	"url" VARCHAR(255),
    FOREIGN KEY ("url") REFERENCES songs("url")
);

CREATE TABLE podcast_episodes (
    "url" VARCHAR(255) PRIMARY KEY,
    episode VARCHAR(255),
    podcast VARCHAR(255)
);

CREATE TABLE podcast_streams (
    stream_id SERIAL PRIMARY KEY,
    time_stamp DATETIME,
    ms_listened INT,
    "url" VARCHAR(255),
    FOREIGN KEY ("url") REFERENCES podcast_episodes("url")
);