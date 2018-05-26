# TODO: use scp instead of filezilla to upload files to gateway!

# Remove folders of the previous run
hdfs dfs -rm -r hadoop-outputs/ex1

# Run application
hadoop jar jars/ex1.jar it.polito.gjcode.hadoop.ex1.Driver data/ex1  hadoop-outputs/ex1