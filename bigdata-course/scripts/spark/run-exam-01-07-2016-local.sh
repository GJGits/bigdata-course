# Remove previous outputs
rm -r ../../spark-local-output/esame_01-07-2016

# Emit new result locally 
spark-submit --class it.polito.gjcode.exam01072016.SparkDriver --deploy-mode client --master local ../../jars/exam-01-07-2016.jar ../../data/esame_01-07-2016/BoughtBooks.txt ../../data/esame_01-07-2016/Books.txt 0.1 ../../spark-local-output/esame_01-07-2016/ex1 ../../spark-local-output/esame_01-07-2016/ex2 