#Remove previous out
rm -r ../../spark-local-output/lab10

# Run
spark-submit --class it.polito.bigdata.spark.SparkDriver --deploy-mode client --master local[*] ../../jars/lab10.jar ../../data/lab10 ../../spark-local-output/ 2>log.txt

#copy to trigger
cp -r ../../data/lab10-tocopy/ ../../data/lab10