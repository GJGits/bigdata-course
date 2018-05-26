# Remove previous outputs
rm -r ../../spark-local-output/ex50sql

# Emit new result locally 
spark-submit --class it.polito.gjcode.spark.ex50.sql.SparkDriver --deploy-mode client --master local ../../jars/ex50-sql.jar ../../data/spark49 ../../spark-local-output/ex50sql