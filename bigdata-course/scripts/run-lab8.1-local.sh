# delete previous submission
rm -r ../spark-local-output/lab8

# submit locally
spark-submit --class it.polito.gjcode.spark.lab7.SparkDriver
--deploy-mode client --master local ../jars/lab8.jar ../data/lab7/registerSample.csv ../data/lab7/stations.csv ../spark-local-output/lab8 0.3