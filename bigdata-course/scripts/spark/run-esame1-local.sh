# Remove previous outputs
rm -r ../../spark-local-output/esame1

# Emit new result locally 
spark-submit --class it.polito.gjcode.spark.esame1.SparkDriver --deploy-mode client --master local ../../jars/esame1-spark.jar ../../data/esame1/preferencesProfile.txt ../../data/esame1/movies.txt  ../../spark-local-output/esame1/es1 ../../spark-local-output/esame1/es2 0.1