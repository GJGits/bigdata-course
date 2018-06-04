# Remove folders of the previous run
rm -rf ../../spark-local-output/wordStream1

# Run application
spark-submit  --class it.polito.gjcode.spark.streaming.wordBase.SparkDriver --deploy-mode client --master local[10] ../../jars/SparkStreamingWordCountWindow-1.0.0.jar ../../spark-local-output/wordStream1

