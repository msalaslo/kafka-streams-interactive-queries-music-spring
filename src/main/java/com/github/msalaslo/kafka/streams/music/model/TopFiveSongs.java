package com.github.msalaslo.kafka.streams.music.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import io.confluent.examples.streams.avro.SongPlayCount;

/**
 * Used in aggregations to keep track of the Top five songs
 */
public class TopFiveSongs implements Iterable<SongPlayCount> {
	private final Map<Long, SongPlayCount> currentSongs = new HashMap<>();

	private final TreeSet<SongPlayCount> topFive = new TreeSet<>((o1, o2) -> {
		final int result = o2.getPlays().compareTo(o1.getPlays());
		if (result != 0) {
			return result;
		}
		return o1.getSongId().compareTo(o2.getSongId());
	});

	public void add(final SongPlayCount songPlayCount) {
		if (currentSongs.containsKey(songPlayCount.getSongId())) {
			topFive.remove(currentSongs.remove(songPlayCount.getSongId()));
		}
		topFive.add(songPlayCount);
		currentSongs.put(songPlayCount.getSongId(), songPlayCount);
		if (topFive.size() > 5) {
			final SongPlayCount last = topFive.last();
			currentSongs.remove(last.getSongId());
			topFive.remove(last);
		}
	}

	public void remove(final SongPlayCount value) {
		topFive.remove(value);
		currentSongs.remove(value.getSongId());
	}

	@Override
	public Iterator<SongPlayCount> iterator() {
		return topFive.iterator();
	}
}
