# delete previous submission
rm -r ../spark-local-output/lab8.2

# submit locally
spark-submit --class it.polito.gjcode.spark.lab8.SparkDriver2 --deploy-mode client --master local ../jars/lab8.jar ../data/lab7/registerSample.csv ../data/lab7/stations.csv ../spark-local-output/lab8.2 0.3