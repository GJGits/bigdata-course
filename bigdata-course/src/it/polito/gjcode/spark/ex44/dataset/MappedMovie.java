package it.polito.gjcode.spark.ex44.dataset;

import java.io.Serializable;

public class MappedMovie implements Serializable {

	private static final long serialVersionUID = 1L;
	private String movieid;
	private String movie_genre;

	public MappedMovie(String movieid, String movie_genre) {
		this.movieid = movieid;
		this.movie_genre = movie_genre;
	}

	public String getMovieid() {
		return movieid;
	}

	public void setMovieid(String movieid) {
		this.movieid = movieid;
	}

	public String getMovie_genre() {
		return movie_genre;
	}

	public void setMovie_genre(String movie_genre) {
		this.movie_genre = movie_genre;
	}

}
