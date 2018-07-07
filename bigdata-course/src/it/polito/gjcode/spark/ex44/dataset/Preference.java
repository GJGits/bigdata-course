package it.polito.gjcode.spark.ex44.dataset;

import java.io.Serializable;

public class Preference implements Serializable {

	private static final long serialVersionUID = 1L;
	private String userid, movie_genre;

	public Preference(String userid, String movie_genre) {
		this.userid = userid;
		this.movie_genre = movie_genre;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getMovie_genre() {
		return movie_genre;
	}

	public void setMovie_genre(String movie_genre) {
		this.movie_genre = movie_genre;
	}

}
