package it.polito.gjcode.exam30062017;

import org.apache.spark.api.java.*;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathPrices;
		String outputPathPartA;
		String outputPathPartB;
		int nw;

		inputPathPrices = args[0];
		nw = Integer.parseInt(args[1]);
		outputPathPartA = args[2];
		outputPathPartB = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2016_06_30 - Exercise #2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of Prices.txt
		JavaRDD<String> prices = sc.textFile(inputPathPrices);

		// Select data of year 2016
		JavaRDD<String> prices2016 = prices.filter(new Year2016());

		// Extract one pair (stockId_date, price) for each input line
		JavaPairRDD<String, Double> stockDate_Price = prices2016.mapToPair(new stockDatePrice()).cache();

		// Select the lowest price for each pair (stockId, date)
		JavaPairRDD<String, Double> stockDate_LowestPrice = stockDate_Price.reduceByKey(new LowestPrice());

		JavaPairRDD<String, Double> sortedStockDate_LowestPrice = stockDate_LowestPrice.sortByKey();

		// Store the selected users in the output folder
		sortedStockDate_LowestPrice.saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// stockDate_Price already contains one element for each pair
		// (stockId_date, price) for each input line

		// Select the highest price for each pair (stockId, date)
		JavaPairRDD<String, Double> stockDate_HighestPrice = stockDate_Price.reduceByKey(new HighestPrice());

		// Map each input pair to a new pair
		// Input pair: (stockId_date, highest_price)
		// New pair: ((stockId_weekNumber, date_highestPrice)
		JavaPairRDD<String, DatePrice> stockId_WeekNumber_date_Price = stockDate_HighestPrice
				.mapToPair(new StockWeek_DatePrice());

		// Group values by key
		JavaPairRDD<String, Iterable<DatePrice>> groupBystockId_WeekNumber = stockId_WeekNumber_date_Price.groupByKey();

		// Select pairs associate with positive weeks
		JavaPairRDD<String, Iterable<DatePrice>> stockId_WeekNumber_PositiveWeeks = groupBystockId_WeekNumber
				.filter(new PositiveWeeks());

		// Count the number of positive weeks for each stock

		// Map each input pair to a new pair
		// Input pair: (stockId_WeekNumber, DatePrice)
		// New pair: ((stockId, 1)
		JavaPairRDD<String, Integer> stockId_One = stockId_WeekNumber_PositiveWeeks.mapToPair(new StockIdOne());

		// Count ones per stock
		JavaPairRDD<String, Integer> stockId_Count = stockId_One.reduceByKey(new Sum());

		// Filter stocks base on the number of "positive weeks"
		JavaPairRDD<String, Integer> frequentStockId_Count = stockId_Count.filter(new Frequent(nw));

		frequentStockId_Count.keys().saveAsTextFile(outputPathPartB);

		sc.close();
	}
}
