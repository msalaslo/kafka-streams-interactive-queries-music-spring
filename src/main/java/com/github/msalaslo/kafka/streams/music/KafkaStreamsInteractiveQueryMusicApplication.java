/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.msalaslo.kafka.streams.music;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.github.msalaslo.kafka.streams.music.config.ApplicationConfiguration;
import com.github.msalaslo.kafka.streams.music.model.TopFiveSongs;
import com.github.msalaslo.kafka.streams.music.serde.TopFiveSerde;

import io.confluent.examples.streams.avro.PlayEvent;
import io.confluent.examples.streams.avro.Song;
import io.confluent.examples.streams.avro.SongPlayCount;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

@SpringBootApplication
public class KafkaStreamsInteractiveQueryMusicApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsInteractiveQueryMusicApplication.class, args);
	}

	public static class KStreamMusicSampleApplication {

		private static final Long MIN_CHARTABLE_DURATION = 30 * 1000L;
		private static final String SONG_PLAY_COUNT_STORE = "song-play-count";

		

		@Bean
		public BiConsumer<KStream<String, PlayEvent>, KTable<Long, Song>> process() {

			return (s, t) -> {
				// create and configure the SpecificAvroSerdes required in this example
				final Map<String, String> serdeConfig = Collections.singletonMap(
						AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
						ApplicationConfiguration.SCHEMA_REGISTRY_URL);

				final SpecificAvroSerde<PlayEvent> playEventSerde = new SpecificAvroSerde<>();
				playEventSerde.configure(serdeConfig, false);

				final SpecificAvroSerde<Song> keySongSerde = new SpecificAvroSerde<>();
				keySongSerde.configure(serdeConfig, true);

				final SpecificAvroSerde<Song> valueSongSerde = new SpecificAvroSerde<>();
				valueSongSerde.configure(serdeConfig, false);

				final SpecificAvroSerde<SongPlayCount> songPlayCountSerde = new SpecificAvroSerde<>();
				songPlayCountSerde.configure(serdeConfig, false);

				// Accept play events that have a duration >= the minimum
				final KStream<Long, PlayEvent> playsBySongId = s
						.filter((region, event) -> event.getDuration() >= MIN_CHARTABLE_DURATION)
						// repartition based on song id
						.map((key, value) -> KeyValue.pair(value.getSongId(), value));

				// join the plays with song as we will use it later for charting
				final KStream<Long, Song> songPlays = playsBySongId.leftJoin(t, (value1, song) -> song,
						Joined.with(Serdes.Long(), playEventSerde, valueSongSerde));

				// create a state store to track song play counts
				final KTable<Song, Long> songPlayCounts = songPlays
						.groupBy((songId, song) -> song, Grouped.with(keySongSerde, valueSongSerde))
						.count(Materialized.<Song, Long, KeyValueStore<Bytes, byte[]>>as(SONG_PLAY_COUNT_STORE)
								.withKeySerde(valueSongSerde).withValueSerde(Serdes.Long()));

				final TopFiveSerde topFiveSerde = new TopFiveSerde();

				// Compute the top five charts for each genre. The results of this computation
				// will continuously update the state
				// store "top-five-songs-by-genre", and this state store can then be queried
				// interactively via a REST API (cf.
				// MusicPlaysRestService) for the latest charts per genre.
				songPlayCounts
						.groupBy(
								(song, plays) -> KeyValue.pair(song.getGenre().toLowerCase(),
										new SongPlayCount(song.getId(), plays)),
								Grouped.with(Serdes.String(), songPlayCountSerde))
						// aggregate into a TopFiveSongs instance that will keep track
						// of the current top five for each genre. The data will be available in the
						// top-five-songs-genre store
						.aggregate(TopFiveSongs::new, (aggKey, value, aggregate) -> {
							aggregate.add(value);
							return aggregate;
						}, (aggKey, value, aggregate) -> {
							aggregate.remove(value);
							return aggregate;
						}, Materialized
								.<String, TopFiveSongs, KeyValueStore<Bytes, byte[]>>as(ApplicationConfiguration.TOP_FIVE_SONGS_BY_GENRE_STORE)
								.withKeySerde(Serdes.String()).withValueSerde(topFiveSerde));

				// Compute the top five chart. The results of this computation will continuously
				// update the state
				// store "top-five-songs", and this state store can then be queried
				// interactively via a REST API (cf.
				// MusicPlaysRestService) for the latest charts per genre.
				songPlayCounts
						.groupBy((song, plays) -> KeyValue.pair(ApplicationConfiguration.TOP_FIVE_KEY, new SongPlayCount(song.getId(), plays)),
								Grouped.with(Serdes.String(), songPlayCountSerde))
						.aggregate(TopFiveSongs::new, (aggKey, value, aggregate) -> {
							aggregate.add(value);
							return aggregate;
						}, (aggKey, value, aggregate) -> {
							aggregate.remove(value);
							return aggregate;
						}, Materialized
								.<String, TopFiveSongs, KeyValueStore<Bytes, byte[]>>as(
										ApplicationConfiguration.TOP_FIVE_SONGS_STORE)
								.withKeySerde(Serdes.String()).withValueSerde(topFiveSerde));
			};

		}
	}

}
