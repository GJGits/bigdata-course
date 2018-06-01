# Remove previous outputs
rm -r ../../spark-local-output/esame2

# Emit new result locally 
spark-submit --class it.polito.gjcode.exam2.SparkDriver --deploy-mode client --master local ../../jars/esame2.jar ../../data/esame2/PM10Readings.txt ../../spark-local-output/esame2/ex1 ../../spark-local-output/esame2/ex2 0.1