# Remove previous outputs
rm -r ../../spark-local-output/mioEsame2A
rm -r ../../spark-local-output/mioEsame2B

# Emit new result locally 
spark-submit --class it.polito.gjcode.exam260620182.SparkDriver --deploy-mode client --master local ../../jars/mioEsame2.jar ../../data/exam-26-06-20182 7.0 0.1 ../../spark-local-output/mioEsame2A ../../spark-local-output/mioEsame2B