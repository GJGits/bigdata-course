# TODO: use scp instead of filezilla to upload files to gateway!

# Remove folders of the previous run
hdfs dfs -rm -r hadoop-outputs/ex3

# Run application
hadoop jar ../jars/ex3.jar it.polito.gjcode.hadoop.ex3.Driver data/ex3  hadoop-outputs/ex3 50.0