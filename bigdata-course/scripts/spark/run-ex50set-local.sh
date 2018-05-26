# Remove previous outputs
rm -r ../../spark-local-output/ex50set

# Emit new result locally 
spark-submit --class it.polito.gjcode.spark.ex50.dataset.SparkDriver --deploy-mode client --master local ../../jars/ex50-dataset.jar ../../data/spark49 ../../spark-local-output/ex50set