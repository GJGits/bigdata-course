package it.polito.gjcode.spark.ex42;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String questionPath = args[0];
		String answersPath = args[1];
		String ouputPath = args[2];

		SparkConf conf = new SparkConf().setAppName("[Spark app #42]");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> questionsRDD = context.textFile(questionPath);
		JavaRDD<String> answersRDD = context.textFile(answersPath);

		JavaPairRDD<String, String> questionsPairRDD = questionsRDD
				.mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], line.split(",")[2]));

		JavaPairRDD<String, String> answersPairRDD = answersRDD
				.mapToPair(line -> new Tuple2<String, String>(line.split(",")[1], line.split(",")[3]));

		JavaPairRDD<String, Iterable<Tuple2<String, String>>> joinRDD = questionsPairRDD.join(answersPairRDD)
				.groupByKey();

		JavaPairRDD<String, Iterable<String>> qaRDD = joinRDD.mapToPair(element -> {
			Iterable<String> listOfAnswers = null;
			List<String> ansTemp = new LinkedList<>();
			for (Tuple2<String, String> tuple : element._2) {
				ansTemp.add(tuple._2);
			}
			listOfAnswers = ansTemp;
			return new Tuple2<String, Iterable<String>>(element._1, listOfAnswers);
		});

		qaRDD.saveAsTextFile(ouputPath);
		context.close();

	}

}
