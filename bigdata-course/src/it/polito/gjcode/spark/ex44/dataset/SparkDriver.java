package it.polito.gjcode.spark.ex44.dataset;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String moviesPath = args[0];
		String preferecesPath = args[1];
		String wtchedMoviesPath = args[2];
		double misleadingThreshold = Double.parseDouble(args[3]);

		// Create a spark session
		SparkSession session = SparkSession.builder().appName("spark exercise").getOrCreate();

		// Create datasets

		Dataset<WatchedMovie> watchedMovies = session.read().format("csv").option("header", true)
				.option("inferSchema", true).load().as(Encoders.bean(WatchedMovie.class));

		Dataset<Preference> prederences = session.read().format("csv").option("header", true)
				.option("inferSchema", true).load().as(Encoders.bean(Preference.class));

		Dataset<Movie> movies = session.read().format("csv").option("header", true).option("inferSchema", true).load()
				.as(Encoders.bean(Movie.class));

		Dataset<MappedWatch> mappedWatches = watchedMovies.map(watch -> {
			String userid = watch.getUserid();
			String movieid = watch.getMovieid();
			return new MappedWatch(userid, movieid);
		}, Encoders.bean(MappedWatch.class));

		Dataset<MappedMovie> mappedMovies = movies.map(movie -> {
			String movieid = movie.getMovieid();
			String movie_genre = movie.getMovie_genre();
			return new MappedMovie(movieid, movie_genre);
		}, Encoders.bean(MappedMovie.class));
		
		
		
		session.stop();

	}

}
