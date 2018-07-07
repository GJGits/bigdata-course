package it.polito.gjcode.spark.ex44.dataset;

import java.io.Serializable;

public class Movie implements Serializable {

	private static final long serialVersionUID = 1L;
	String movieid, title, movie_genre;
	
	public Movie(String movieid, String title, String movie_genre) {
		this.movieid = movieid;
		this.title = title;
		this.movie_genre = movie_genre;
	}

	public String getMovieid() {
		return movieid;
	}

	public void setMovieid(String movieid) {
		this.movieid = movieid;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getMovie_genre() {
		return movie_genre;
	}

	public void setMovie_genre(String movie_genre) {
		this.movie_genre = movie_genre;
	}
	
	

}
