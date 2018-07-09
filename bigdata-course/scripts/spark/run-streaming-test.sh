# Remove folders of the previous run
rm -rf ../../spark-local-output/streamingTest

# Run application
spark-submit  --class it.polito.gjcode.stramingTest.SparkDriver --deploy-mode client --master local[10] ../../jars/streamingTest.jar ../../spark-local-output/streamingTest/out.csv 2>log.txt