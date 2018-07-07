# Remove previous outputs
rm -r ../../spark-local-output/dataset-test

# Emit new result locally 
spark-submit --class it.polito.gjcode.datasetTest.SparkDriver --deploy-mode client --master local ../../jars/datasetTest.jar ../../data/dataset-test