package com.github.msalaslo.kafka.streams.music.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Miguel Salas
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SongDTO {

	private String artist;
	private String album;
	private String name;
	private String genre;
}
