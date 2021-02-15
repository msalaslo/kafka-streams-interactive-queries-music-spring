package com.github.msalaslo.kafka.streams.music.service;

import java.util.List;

import com.github.msalaslo.kafka.streams.music.api.dto.SongDTO;
import com.github.msalaslo.kafka.streams.music.api.dto.SongPlayCountDTO;

public interface SongService {
	
	public SongDTO getSongById(Long id, String path);
	public List<SongPlayCountDTO> getTopFiveSongs(String path);
	public List<SongPlayCountDTO> getTopFiveSongsByGenre(String path, String genre);

}
