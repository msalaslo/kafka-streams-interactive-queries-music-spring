package com.github.msalaslo.kafka.streams.music.config;

public class ApplicationConfiguration {
	
	public static final String PLAY_EVENTS_INPUT_TOPIC = "play-events";
	public static final String SONG_FEED_INPUT_TOPIC = "song-feed";
	public static final String TOP_FIVE_KEY = "all";
	public static final String TOP_FIVE_SONGS_STORE = "top-five-songs";
	public static final String TOP_FIVE_SONGS_BY_GENRE_STORE = "top-five-songs-by-genre";
	public static final String ALL_SONGS_STORE = "all-songs";

	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

}
