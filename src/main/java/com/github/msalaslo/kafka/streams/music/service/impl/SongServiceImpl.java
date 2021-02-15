package com.github.msalaslo.kafka.streams.music.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.github.msalaslo.kafka.streams.music.api.dto.SongDTO;
import com.github.msalaslo.kafka.streams.music.api.dto.SongPlayCountDTO;
import com.github.msalaslo.kafka.streams.music.config.ApplicationConfiguration;
import com.github.msalaslo.kafka.streams.music.model.TopFiveSongs;
import com.github.msalaslo.kafka.streams.music.service.SongService;

import io.confluent.examples.streams.avro.Song;
import io.confluent.examples.streams.avro.SongPlayCount;

@Service
public class SongServiceImpl implements SongService {

	@Autowired
	private InteractiveQueryService interactiveQueryService;

	private final Log logger = LogFactory.getLog(getClass());

	@Override
	@SuppressWarnings("unchecked")
	public List<SongPlayCountDTO> getTopFiveSongs(String path) {
		HostInfo hostInfo = getHostInfoForTopFiveSongsStore();

		if (interactiveQueryService.getCurrentHostInfo().equals(hostInfo)) {
			logger.info("Top Five songs By genre request served from same host: " + hostInfo);
			TopFiveSongs topFiveSongs = getTopFiveSongsFromStore();

			// Create an empty result list
			final List<SongPlayCountDTO> results = new ArrayList<>();
			// Add the song information to the DTO
			topFiveSongs.forEach(songPlayCount -> {
				addSongInformation(results, songPlayCount);
			});
			return results;

		} else {
			// find the store from the proper instance.
			logger.info("Top Five songs By genre request served from different host: " + hostInfo);
			RestTemplate restTemplate = new RestTemplate();
			return restTemplate.postForObject(String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path),
					"", List.class);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<SongPlayCountDTO> getTopFiveSongsByGenre(String path, String genre) {
		HostInfo hostInfo = getHostInfoForTopFiveSongsByGenreStore(genre);

		if (interactiveQueryService.getCurrentHostInfo().equals(hostInfo)) {
			logger.info("Top Five songs By genre request served from same host: " + hostInfo);
			TopFiveSongs topFiveSongs = getTopFiveSongsByGenreFromStore(genre);

			// Create an empty result list
			final List<SongPlayCountDTO> results = new ArrayList<>();
			// Add the song information to the DTO
			topFiveSongs.forEach(songPlayCount -> {
				addSongInformation(results, songPlayCount);
			});
			return results;

		} else {
			// find the store from the proper instance.
			logger.info("Top Five songs By genre request served from different host: " + hostInfo);
			RestTemplate restTemplate = new RestTemplate();
			return restTemplate.postForObject(String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path),
					genre, List.class);
		}
	}

	private TopFiveSongs getTopFiveSongsFromStore() {
		final ReadOnlyKeyValueStore<String, TopFiveSongs> topFiveStore = interactiveQueryService.getQueryableStore(
				ApplicationConfiguration.TOP_FIVE_SONGS_STORE,
				QueryableStoreTypes.<String, TopFiveSongs>keyValueStore());

		// Get the value from the store
		final TopFiveSongs topFiveSongs = topFiveStore.get(ApplicationConfiguration.TOP_FIVE_KEY);
		if (topFiveSongs == null) {
			throw new IllegalArgumentException(String.format("Unable to find value in %s for key %s",
					ApplicationConfiguration.TOP_FIVE_SONGS_STORE, ApplicationConfiguration.TOP_FIVE_KEY));
		}
		return topFiveSongs;
	}

	private TopFiveSongs getTopFiveSongsByGenreFromStore(String genre) {
		final ReadOnlyKeyValueStore<String, TopFiveSongs> topFiveStore = interactiveQueryService.getQueryableStore(
				ApplicationConfiguration.TOP_FIVE_SONGS_BY_GENRE_STORE,
				QueryableStoreTypes.<String, TopFiveSongs>keyValueStore());

		// Get the value from the store
		final TopFiveSongs topFiveSongs = topFiveStore.get(genre);
		if (topFiveSongs == null) {
			throw new IllegalArgumentException(String.format("Unable to find value in %s for key %s",
					ApplicationConfiguration.TOP_FIVE_SONGS_BY_GENRE_STORE, genre));
		}
		return topFiveSongs;
	}

	private void addSongInformation(List<SongPlayCountDTO> results, SongPlayCount songPlayCount) {
		HostInfo hostInfo = getHostInfoForAllSongsStore(songPlayCount.getSongId());
		SongPlayCountDTO songPlaycountDTO = null;
		if (interactiveQueryService.getCurrentHostInfo().equals(hostInfo)) {
			logger.info("Song info request served from same host: " + hostInfo);

			final ReadOnlyKeyValueStore<Long, Song> songStore = interactiveQueryService.getQueryableStore(
					ApplicationConfiguration.ALL_SONGS_STORE, QueryableStoreTypes.<Long, Song>keyValueStore());

			final Song song = songStore.get(songPlayCount.getSongId());
			songPlaycountDTO = SongPlayCountDTO.builder().artist(song.getArtist()).album(song.getAlbum())
					.name(song.getName()).genre(song.getGenre()).plays(songPlayCount.getPlays()).build();

		} else {
			logger.info("Song info request served from different host: " + hostInfo);
			RestTemplate restTemplate = new RestTemplate();
			SongDTO song = restTemplate.postForObject(
					String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), "song/idx?id="), "id",
					SongDTO.class);
			songPlaycountDTO = SongPlayCountDTO.builder().artist(song.getArtist()).album(song.getAlbum())
					.name(song.getName()).genre(song.getGenre()).plays(songPlayCount.getPlays()).build();
		}
		results.add(songPlaycountDTO);
	}

	private HostInfo getHostInfoForTopFiveSongsByGenreStore(String genre) {
		return interactiveQueryService.getHostInfo(ApplicationConfiguration.TOP_FIVE_SONGS_BY_GENRE_STORE, genre,
				new StringSerializer());
	}

	private HostInfo getHostInfoForAllSongsStore(Long songId) {
		return interactiveQueryService.getHostInfo(ApplicationConfiguration.ALL_SONGS_STORE, songId,
				new LongSerializer());
	}

	private HostInfo getHostInfoForTopFiveSongsStore() {
		return interactiveQueryService.getHostInfo(ApplicationConfiguration.TOP_FIVE_SONGS_STORE,
				ApplicationConfiguration.TOP_FIVE_KEY, new StringSerializer());
	}

	@Override
	public SongDTO getSongById(Long id, String path) {
		HostInfo hostInfo = getHostInfoForAllSongsStore(id);
		SongDTO songDTO = null;
		if (interactiveQueryService.getCurrentHostInfo().equals(hostInfo)) {
			logger.info("Song info request served from same host: " + hostInfo);

			final ReadOnlyKeyValueStore<Long, Song> songStore = interactiveQueryService.getQueryableStore(
					ApplicationConfiguration.ALL_SONGS_STORE, QueryableStoreTypes.<Long, Song>keyValueStore());

			final Song song = songStore.get(id);
			songDTO = SongDTO.builder().artist(song.getArtist()).album(song.getAlbum()).name(song.getName())
					.genre(song.getGenre()).build();

		} else {
			logger.info("Song info request served from different host: " + hostInfo);
			RestTemplate restTemplate = new RestTemplate();
			SongDTO song = restTemplate.postForObject(
					String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path), "id", SongDTO.class);
			songDTO = SongDTO.builder().artist(song.getArtist()).album(song.getAlbum()).name(song.getName())
					.genre(song.getGenre()).build();
		}
		return songDTO;
	}
}
