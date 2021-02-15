package com.github.msalaslo.kafka.streams.music.api.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.msalaslo.kafka.streams.music.api.dto.SongDTO;
import com.github.msalaslo.kafka.streams.music.api.dto.SongPlayCountDTO;
import com.github.msalaslo.kafka.streams.music.service.SongService;

@RestController
public class MusicController {
	
	@Autowired
	private SongService songService;

	@RequestMapping("/song/idx")
	public SongDTO song(@RequestParam(value = "id") Long id) {
		return songService.getSongById(id, "song/idx?id=" + id);
	}

	@RequestMapping("/charts/top-five")
	public List<SongPlayCountDTO> topFive(@RequestParam(value = "genre", required = false) String genre) {
		if(genre == null) {
			return songService.getTopFiveSongs("charts/top-five");
		}else {
			return songService.getTopFiveSongsByGenre("charts/top-five?genre=" + genre, genre);
		}
		
	}

}
