# Remove previous outputs
rm -r ../../spark-local-output/ex49sql

# Emit new result locally 
spark-submit --class it.polito.gjcode.spark.ex49.sql.SparkDriver --deploy-mode client --master local ../../jars/ex49-sql.jar ../../data/spark49 ../../spark-local-output/ex49sql  