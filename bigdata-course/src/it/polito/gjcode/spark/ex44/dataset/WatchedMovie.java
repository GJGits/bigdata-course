package it.polito.gjcode.spark.ex44.dataset;

import java.io.Serializable;
import java.sql.Timestamp;

public class WatchedMovie implements Serializable {

	private static final long serialVersionUID = 1L;
	private String userid, movieid;
	private Timestamp start_timestamp, end_timestamp;
	
	public WatchedMovie(String userid, String movieid, Timestamp start_timestamp, Timestamp end_timestamp) {
		this.userid = userid;
		this.movieid = movieid;
		this.start_timestamp = start_timestamp;
		this.end_timestamp = end_timestamp;
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

	public Timestamp getStart_timestamp() {
		return start_timestamp;
	}

	public void setStart_timestamp(Timestamp start_timestamp) {
		this.start_timestamp = start_timestamp;
	}

	public Timestamp getEnd_timestamp() {
		return end_timestamp;
	}

	public void setEnd_timestamp(Timestamp end_timestamp) {
		this.end_timestamp = end_timestamp;
	}

}
