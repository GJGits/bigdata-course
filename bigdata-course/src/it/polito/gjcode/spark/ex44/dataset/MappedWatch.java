package it.polito.gjcode.spark.ex44.dataset;

import java.io.Serializable;

public class MappedWatch implements Serializable {

	private static final long serialVersionUID = 1L;
	private String userid, movieid;
	
	public MappedWatch(String userid, String movied) {
		this.userid = userid;
		movieid = movied;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getMovieid() {
		return movieid;
	}

	public void setMovieid(String movieid) {
		this.movieid = movieid;
	}
	
	

}
