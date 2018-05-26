# delete previous submission
rm -r ../spark-local-output/lab7

# submit locally
spark-submit --class it.polito.gjcode.spark.lab7.SparkDriver
--deploy-mode client --master local ../jars/lab7.jar ../data/lab7/registerSample.csv ../data/lab7/stations.csv 0.3 ../spark-local-output/lab7