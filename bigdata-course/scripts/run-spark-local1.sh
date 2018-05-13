# Remove folders of the previous run
rm -rf ../spark-local-output/spark1

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise30.SparkDriver --deploy-mode client --master local ../jars/spark1.jar "../data/spark1/log.txt" ../spark-local-output/spark1