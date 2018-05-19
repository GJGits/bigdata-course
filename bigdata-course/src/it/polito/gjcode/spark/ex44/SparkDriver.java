package it.polito.gjcode.spark.ex44;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String watchedMoviesPath = args[0];
		String preferencesPath = args[1];
		String movieInfosPath = args[2];
		String outputPath = args[3];
		double trustTreshold = Double.valueOf(args[4].trim());

		SparkConf conf = new SparkConf().setAppName("[Spark app ex44]");
		JavaSparkContext context = new JavaSparkContext(conf);

		/* Create the users watched movies RDD */
		JavaRDD<String> watchedRDD = context.textFile(watchedMoviesPath);

		/* Create the user preferences RDD */
		JavaRDD<String> preferencesRDD = context.textFile(preferencesPath);

		/* Create the movieInfos RDD */
		JavaRDD<String> movieInfoRDD = context.textFile(movieInfosPath);

		/*
		 * Create a pair RDD in the format: <userId> <list of movies-genre>. Each line
		 * indicate the list of movie genre preferences of the user.
		 */

		JavaPairRDD<String, Iterable<String>> userPreferencesRDD = preferencesRDD.mapToPair(line -> {

			String[] lineTokens = line.split(",");
			String userId = lineTokens[0];
			String genre = lineTokens[1];
			return new Tuple2<String, String>(userId, genre);

		}).groupByKey();

		/*
		 * Create a pair RDD in the format: <userId> <movieId>. Every line indicate that
		 * userid watched movie with an id equals to movieId.
		 */

		JavaPairRDD<String, String> userWatchedMovieRDD = watchedRDD.mapToPair(line -> {

			String[] lineTokens = line.split(",");
			String userId = lineTokens[0];
			String movieId = lineTokens[1];
			return new Tuple2<String, String>(movieId, userId);

		});

		/* Create a pair RDD in the format: <movieId> <genre>. Self explaining. */

		JavaPairRDD<String, String> movieGenreRDD = movieInfoRDD.mapToPair(line -> {

			String[] lineTokens = line.split(",");
			String movieId = lineTokens[0];
			String genre = lineTokens[1];
			return new Tuple2<String, String>(movieId, genre);

		});

		/*
		 * Join views to retrieve the info userId watched the film with a certain id and
		 * genre.
		 */

		JavaPairRDD<String, String> userMovieInfo = userWatchedMovieRDD.join(movieGenreRDD).mapToPair(element -> {

			String userId = element._2._1;
			String movieId = element._1;
			String genre = element._2._2;
			String movieInfo = String.join("-", movieId, genre);

			return new Tuple2<String, String>(userId, movieInfo);

		});

		/*
		 * Join the userMovieInfo with the userPreferences and retrieve the final
		 * result.
		 */

		JavaRDD<String> resultRDD = userPreferencesRDD.join(userMovieInfo).mapToPair(element -> {

			String userId = element._1;
			boolean matchFound = false;

			// iterate over preferences (element._2._1 = iterable of strings representing
			// the preferences).

			for (String preference : element._2._1) {
				String genre = element._2._2;
				if (preference.compareTo(genre) == 0)
					matchFound = true;
			}

			Counter counter = matchFound ? new Counter(1, 1) : new Counter(0, 1);
			return new Tuple2<String, Counter>(userId, counter);

		}).reduceByKey((Counter count1, Counter count2) -> {

			int newSum = count1.getSum() + count2.getSum();
			int newCount = count1.getCount() + count2.getCount();
			Counter counter = new Counter(newSum, newCount);
			return counter;

		}).filter(element -> {

			double sum = element._2.getSum();
			double count = element._2.getCount();
			double percentage = (double) (sum / count);
			return percentage > trustTreshold;

		}).map(element -> {

			String userId = element._1;
			return userId;

		});

		resultRDD.saveAsTextFile(outputPath);

		/* Close the context. */
		context.close();

	}

}
